package output

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/marre/ksink/pkg/ksink"
)

// durableS3Buffer uses directory renames as a small write-ahead state
// machine. A journal is updated only after the data file has been synced.
type durableS3Buffer struct {
	mu       sync.Mutex
	root     string
	bucket   string
	keyTpl   string
	client   s3API
	opts     S3Opts
	active   map[string]*durableEntry
	stop     chan struct{}
	done     chan struct{}
	closeOne sync.Once
	asyncErr error
}

// durableAgeCheckDivisor checks twice per age window, limiting rotation delay
// without creating a high-frequency background loop.
const durableAgeCheckDivisor = 2

type durableEntry struct {
	key       string
	objectKey string
	dataPath  string
	metaPath  string
	file      *os.File
	count     int
	offset    int64
	created   time.Time
}

type durableMeta struct {
	Bucket    string    `json:"bucket"`
	Key       string    `json:"key"`
	KeyTpl    string    `json:"key_template"`
	ObjectKey string    `json:"object_key"`
	Count     int       `json:"count"`
	Offset    int64     `json:"offset"`
	Created   time.Time `json:"created"`
}

func newDurableS3Buffer(root, bucket, keyTpl string, client s3API, opts S3Opts) (*durableS3Buffer, error) {
	b := &durableS3Buffer{
		root: root, bucket: bucket, keyTpl: keyTpl, client: client, opts: opts,
		active: make(map[string]*durableEntry), stop: make(chan struct{}), done: make(chan struct{}),
	}
	for _, state := range []string{"active", "pending", "uploading"} {
		if err := os.MkdirAll(filepath.Join(root, state), 0700); err != nil {
			return nil, fmt.Errorf("failed to create S3 buffer directory: %w", err)
		}
	}
	if err := b.recover(); err != nil {
		return nil, err
	}
	if opts.BatchMaxAge > 0 {
		go b.ageLoop()
	} else {
		close(b.done)
	}
	return b, nil
}

func (b *durableS3Buffer) write(data []byte, msg *ksink.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.asyncErr != nil {
		return b.asyncErr
	}
	key := resolveS3Key(b.keyTpl, "", topicFromMsg(msg))
	e := b.active[key]
	if e == nil {
		var err error
		e, err = b.openEntry(key)
		if err != nil {
			return err
		}
		b.active[key] = e
	}
	n, err := e.file.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write durable S3 buffer: %w", err)
	}
	if err := e.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync durable S3 buffer: %w", err)
	}
	e.offset += int64(n)
	e.count++
	if err := b.writeMeta(e); err != nil {
		return err
	}
	if e.offset >= int64(b.opts.BatchMaxBytes) || e.count >= b.opts.BatchMaxMessages {
		return b.rotateLocked(key)
	}
	return nil
}

func (b *durableS3Buffer) flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.asyncErr != nil {
		return b.asyncErr
	}
	for key := range b.active {
		if err := b.rotateLocked(key); err != nil {
			return err
		}
	}
	return b.syncPendingLocked()
}

func (b *durableS3Buffer) close() error {
	var err error
	b.closeOne.Do(func() {
		close(b.stop)
		<-b.done
		err = b.flush()
	})
	return err
}

func (b *durableS3Buffer) ageLoop() {
	// Check at intervals of BatchMaxAge/durableAgeCheckDivisor.
	t := time.NewTicker(b.opts.BatchMaxAge / durableAgeCheckDivisor)
	defer func() { t.Stop(); close(b.done) }()
	for {
		select {
		case <-t.C:
			b.mu.Lock()
			now := time.Now()
			for key, e := range b.active {
				if now.Sub(e.created) >= b.opts.BatchMaxAge {
					if err := b.rotateLocked(key); err != nil && b.asyncErr == nil {
						b.asyncErr = err
					}
				}
			}
			if err := b.syncPendingLocked(); err != nil && b.asyncErr == nil {
				b.asyncErr = err
			}
			b.mu.Unlock()
		case <-b.stop:
			return
		}
	}
}

func (b *durableS3Buffer) openEntry(key string) (*durableEntry, error) {
	// Hashing keeps arbitrary S3 keys out of local path names and provides a
	// stable collision-resistant filename for recovery.
	keyHash := sha256.Sum256([]byte(key))
	hashedFileName := hex.EncodeToString(keyHash[:])
	dir := filepath.Join(b.root, "active")
	dataPath := filepath.Join(dir, hashedFileName+".data")
	metaPath := dataPath + ".meta"
	f, err := os.OpenFile(dataPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open durable S3 buffer: %w", err)
	}
	e := &durableEntry{
		key: key, objectKey: addS3BatchSuffix(key, uint64(time.Now().UnixNano())),
		dataPath: dataPath, metaPath: metaPath, file: f, created: time.Now(),
	}
	if err := b.writeMeta(e); err != nil {
		_ = f.Close()
		return nil, err
	}
	return e, nil
}

func (b *durableS3Buffer) writeMeta(e *durableEntry) error {
	meta := durableMeta{Bucket: b.bucket, Key: e.key, KeyTpl: b.keyTpl, ObjectKey: e.objectKey, Count: e.count, Offset: e.offset, Created: e.created}
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("failed to encode durable S3 journal: %w", err)
	}
	tmp := e.metaPath + ".new"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return fmt.Errorf("failed to write durable S3 journal: %w", err)
	}
	if err := os.Rename(tmp, e.metaPath); err != nil {
		return fmt.Errorf("failed to commit durable S3 journal: %w", err)
	}
	return nil
}

func (b *durableS3Buffer) rotateLocked(key string) error {
	e := b.active[key]
	if e == nil {
		return nil
	}
	if err := e.file.Sync(); err != nil {
		return err
	}
	if err := e.file.Close(); err != nil {
		return err
	}
	delete(b.active, key)
	pendingData := filepath.Join(b.root, "pending", filepath.Base(e.dataPath))
	pendingMeta := pendingData + ".meta"
	if err := os.Rename(e.dataPath, pendingData); err != nil {
		return fmt.Errorf("failed to move durable S3 buffer to pending: %w", err)
	}
	if err := os.Rename(e.metaPath, pendingMeta); err != nil {
		return fmt.Errorf("failed to move durable S3 journal to pending: %w", err)
	}
	e.dataPath, e.metaPath = pendingData, pendingMeta
	return b.syncPendingLocked()
}

func (b *durableS3Buffer) syncPendingLocked() error {
	for _, state := range []string{"pending", "uploading"} {
		dir := filepath.Join(b.root, state)
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, item := range entries {
			if item.IsDir() || filepath.Ext(item.Name()) != ".data" {
				continue
			}
			if err := b.upload(filepath.Join(dir, item.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *durableS3Buffer) upload(dataPath string) error {
	metaPath := dataPath + ".meta"
	metaData, err := os.ReadFile(metaPath)
	if err != nil {
		return fmt.Errorf("missing journal for durable S3 buffer %s: %w", dataPath, err)
	}
	var meta durableMeta
	if err := json.Unmarshal(metaData, &meta); err != nil {
		return fmt.Errorf("invalid journal for durable S3 buffer %s: %w", dataPath, err)
	}
	if meta.Bucket != b.bucket {
		return fmt.Errorf("bucket mismatch: file %s has bucket %q but expected %q", dataPath, meta.Bucket, b.bucket)
	}
	if meta.KeyTpl != b.keyTpl {
		return fmt.Errorf("key template mismatch: file %s has %q but expected %q", dataPath, meta.KeyTpl, b.keyTpl)
	}
	if err := validateDurableFile(dataPath, meta); err != nil {
		return err
	}
	uploadPath := filepath.Join(b.root, "uploading", filepath.Base(dataPath))
	uploadMeta := uploadPath + ".meta"
	if filepath.Dir(dataPath) != filepath.Dir(uploadPath) {
		if err := os.Rename(dataPath, uploadPath); err != nil {
			return err
		}
		if err := os.Rename(metaPath, uploadMeta); err != nil {
			return err
		}
		dataPath, metaPath = uploadPath, uploadMeta
	}
	f, err := os.Open(dataPath)
	if err != nil {
		return err
	}
	if err := withS3Retry(b.opts, func() error {
		_, e := b.client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(meta.Bucket), Key: aws.String(meta.ObjectKey), Body: f,
		})
		if e != nil {
			_, _ = f.Seek(0, io.SeekStart)
		}
		return e
	}); err != nil {
		_ = f.Close()
		return fmt.Errorf("failed to sync durable S3 buffer %s: %w", dataPath, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close durable S3 buffer %s: %w", dataPath, err)
	}
	if err := os.Remove(dataPath); err != nil {
		return err
	}
	if err := os.Remove(metaPath); err != nil {
		return err
	}
	return nil
}

func (b *durableS3Buffer) recover() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	active := filepath.Join(b.root, "active")
	entries, err := os.ReadDir(active)
	if err != nil {
		return err
	}
	for _, item := range entries {
		if item.IsDir() || filepath.Ext(item.Name()) != ".data" {
			continue
		}
		dataPath := filepath.Join(active, item.Name())
		metaPath := dataPath + ".meta"
		metaData, err := os.ReadFile(metaPath)
		if err != nil {
			return fmt.Errorf("cannot recover %s: %w", dataPath, err)
		}
		var meta durableMeta
		if err := json.Unmarshal(metaData, &meta); err != nil {
			return fmt.Errorf("cannot recover %s: %w", dataPath, err)
		}
		if err := validateDurableFile(dataPath, meta); err != nil {
			return fmt.Errorf("cannot recover %s: %w", dataPath, err)
		}
		if err := os.Truncate(dataPath, meta.Offset); err != nil {
			return err
		}
		pendingData := filepath.Join(b.root, "pending", item.Name())
		if err := os.Rename(dataPath, pendingData); err != nil {
			return err
		}
		if err := os.Rename(metaPath, pendingData+".meta"); err != nil {
			return err
		}
	}
	return b.syncPendingLocked()
}

func validateDurableFile(dataPath string, meta durableMeta) error {
	info, err := os.Stat(dataPath)
	if err != nil {
		return err
	}
	if info.Size() < meta.Offset {
		return fmt.Errorf("file size (%d bytes) is shorter than journal offset (%d bytes)", info.Size(), meta.Offset)
	}
	return nil
}
