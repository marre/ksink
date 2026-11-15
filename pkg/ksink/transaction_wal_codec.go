package ksink

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

const (
	walMetadataTag byte = 1
	walOutputTag byte = 2
	walTopicOutputTag byte = 3
	walOutputHeaderBytes = 1 + 4 + 2 + 4 + 4
	// tag + txnID len + instance len + topic len + topic offset + data len
	walTopicOutputHeaderBytes = 1 + 4 + 2 + 4 + 8 + 4
)

// encodeTxnWALRecord stores output bytes without JSON/base64 expansion.
// Metadata records remain versioned JSON. Every record has a self-identifying,
// checksum-protected envelope (including header fields and monotonic LSN).
func encodeTxnWALRecord(rec transactionWALRecord) ([]byte, error) {
	rec.Version = transactionWALVersion
	if rec.Type == "topic-output" {
		if rec.TopicOutput == nil { return nil, errors.New("missing topic WAL output") }
		o := rec.TopicOutput
		if len(o.TransactionID) > 1<<20 || len(o.Instance) > 65535 || len(o.Topic) > 249 {
			return nil, errors.New("topic WAL identity too long")
		}
		total := uint64(walTopicOutputHeaderBytes) + uint64(len(o.TransactionID)) +
			uint64(len(o.Instance)) + uint64(len(o.Topic)) + uint64(len(o.Data))
		if total > transactionWALMaxFrame { return nil, fmt.Errorf("topic WAL output exceeds %d bytes", transactionWALMaxFrame) }
		out := make([]byte, int(total))
		out[0] = walTopicOutputTag
		binary.BigEndian.PutUint32(out[1:5], uint32(len(o.TransactionID)))
		binary.BigEndian.PutUint16(out[5:7], uint16(len(o.Instance)))
		binary.BigEndian.PutUint32(out[7:11], uint32(len(o.Topic)))
		binary.BigEndian.PutUint64(out[11:19], uint64(o.Offset))
		binary.BigEndian.PutUint32(out[19:23], uint32(len(o.Data)))
		pos := walTopicOutputHeaderBytes
		for _, field := range []string{o.TransactionID, o.Instance, o.Topic} {
			pos += copy(out[pos:], field)
		}
		copy(out[pos:], o.Data)
		return out, nil
	}
	if rec.Type != "output" {
		jsonBytes, err := json.Marshal(rec)
		if err != nil { return nil, err }
		if len(jsonBytes)+1 > transactionWALMaxFrame {
			return nil, fmt.Errorf("WAL metadata record exceeds %d bytes", transactionWALMaxFrame)
		}
		return append([]byte{walMetadataTag}, jsonBytes...), nil
	}
	if rec.Output == nil { return nil, errors.New("missing WAL output payload") }
	o := rec.Output
	if len(o.TransactionID) > (1<<20) || len(o.Instance) > 65535 || len(o.Path) > (1<<20) {
		return nil, errors.New("WAL output path or transaction identity too long")
	}
	total := uint64(walOutputHeaderBytes) + uint64(len(o.TransactionID)) +
		uint64(len(o.Instance)) + uint64(len(o.Path)) + uint64(len(o.Data))
	if total > transactionWALMaxFrame { return nil, fmt.Errorf("WAL output exceeds %d bytes", transactionWALMaxFrame) }
	out := make([]byte, int(total))
	out[0] = walOutputTag
	binary.BigEndian.PutUint32(out[1:5], uint32(len(o.TransactionID)))
	binary.BigEndian.PutUint16(out[5:7], uint16(len(o.Instance)))
	binary.BigEndian.PutUint32(out[7:11], uint32(len(o.Path)))
	binary.BigEndian.PutUint32(out[11:15], uint32(len(o.Data)))
	pos := walOutputHeaderBytes
	for _, field := range []string{o.TransactionID, o.Instance, o.Path} {
		pos += copy(out[pos:], field)
	}
	copy(out[pos:], o.Data)
	return out, nil
}

// decodeTxnWALRecord checks internal lengths before slicing or allocating.
// Corrupt complete records fail replay rather than being treated as a torn
// unacknowledged tail.
func decodeTxnWALRecord(raw []byte) (transactionWALRecord, error) {
	if len(raw)==0 { return transactionWALRecord{}, errors.New("empty WAL record") }
	if raw[0] == walMetadataTag {
		var rec transactionWALRecord
		if err := json.Unmarshal(raw[1:], &rec); err != nil { return rec, err }
		if rec.Type=="output" { return rec, errors.New("WAL output must be binary") }
		if rec.Version != transactionWALVersion { return rec, fmt.Errorf("unknown WAL version %d", rec.Version) }
		return rec, nil
	}
	if raw[0] == walTopicOutputTag {
		if len(raw) < walTopicOutputHeaderBytes { return transactionWALRecord{}, errors.New("truncated binary topic WAL header") }
		idLen := binary.BigEndian.Uint32(raw[1:5])
		instanceLen := uint32(binary.BigEndian.Uint16(raw[5:7]))
		topicLen := binary.BigEndian.Uint32(raw[7:11])
		offset := binary.BigEndian.Uint64(raw[11:19])
		payloadLen := binary.BigEndian.Uint32(raw[19:23])
		total := uint64(walTopicOutputHeaderBytes) + uint64(idLen)+uint64(instanceLen)+uint64(topicLen)+uint64(payloadLen)
		if total!=uint64(len(raw)) || idLen>(1<<20) || topicLen>249 || offset>uint64(1<<63-1) {
			return transactionWALRecord{}, errors.New("invalid binary topic WAL field lengths or offset")
		}
		pos := walTopicOutputHeaderBytes
		id := string(raw[pos:pos+int(idLen)]); pos+=int(idLen)
		instance := string(raw[pos:pos+int(instanceLen)]); pos+=int(instanceLen)
		topic := string(raw[pos:pos+int(topicLen)]); pos+=int(topicLen)
		return transactionWALRecord{Version:transactionWALVersion, Type:"topic-output", TopicOutput:&WALTopicRecord{
			Topic:topic, Offset:int64(offset), TransactionID:id, Instance:instance, Data:raw[pos:],
		}}, nil
	}
	if raw[0] != walOutputTag { return transactionWALRecord{}, fmt.Errorf("unknown WAL tag %d", raw[0]) }
	if len(raw) < walOutputHeaderBytes { return transactionWALRecord{}, errors.New("truncated binary WAL header") }
	idLen := binary.BigEndian.Uint32(raw[1:5])
	instanceLen := uint32(binary.BigEndian.Uint16(raw[5:7]))
	pathLen := binary.BigEndian.Uint32(raw[7:11])
	payloadLen := binary.BigEndian.Uint32(raw[11:15])
	total := uint64(walOutputHeaderBytes) + uint64(idLen)+uint64(instanceLen)+uint64(pathLen)+uint64(payloadLen)
	if total!=uint64(len(raw)) || idLen>(1<<20) || pathLen>(1<<20) {
		return transactionWALRecord{}, fmt.Errorf("invalid binary WAL output field lengths")
	}
	pos := walOutputHeaderBytes
	id := string(raw[pos:pos+int(idLen)]);pos+=int(idLen)
	instance := string(raw[pos:pos+int(instanceLen)]);pos+=int(instanceLen)
	path := string(raw[pos:pos+int(pathLen)]);pos+=int(pathLen)
	return transactionWALRecord{Version: transactionWALVersion, Type: "output", Output: &WALOutputRecord{
		TransactionID:id, Instance:instance, Path:path, Data:raw[pos:],
	}}, nil
}

// WAL v5 physical frame header: magic[4], version[2], header length[2],
// sequence[8], payload length[4], payload CRC32C[4], header CRC32C[4].
// All integers are big-endian. Sequence is strictly increasing for frames
// in physical WAL replay order (including checkpoint generations).
const (
    txnWALMagic = "KSWL"
    txnWALFrameVersion uint16 = 1
    txnWALHeaderBytes = 28
)

func txnWALHeader(data []byte) (uint32,uint64,error) {
    if len(data)!=txnWALHeaderBytes {return 0,0,fmt.Errorf("truncated WAL frame header")}
    if string(data[:4])!=txnWALMagic {return 0,0,errors.New("unrecognized WAL frame magic")}
    if v:=binary.BigEndian.Uint16(data[4:6]);v!=txnWALFrameVersion {
        return 0,0,fmt.Errorf("unsupported WAL frame version %d",v)
    }
    if n:=binary.BigEndian.Uint16(data[6:8]);n!=txnWALHeaderBytes {
        return 0,0,fmt.Errorf("unsupported WAL frame header size %d",n)
    }
    if crc32.Checksum(data[:24],transactionWALCRCTable)!=binary.BigEndian.Uint32(data[24:28]){
        return 0,0,errors.New("WAL frame header checksum mismatch")
    }
    n:=binary.BigEndian.Uint32(data[16:20])
    if n==0 || n>transactionWALMaxFrame{return 0,0,fmt.Errorf("invalid WAL record length %d",n)}
    seq:=binary.BigEndian.Uint64(data[8:16])
    if seq==0 || seq==^uint64(0) {return 0,0,fmt.Errorf("invalid WAL frame sequence %d",seq)}
    return n,seq,nil
}

func txnWALFrameWithSequence(rec transactionWALRecord,seq uint64) ([]byte,error) {
    payload,err:=encodeTxnWALRecord(rec)
    if err!=nil{return nil,err}
    if seq==0 || seq==^uint64(0){return nil,fmt.Errorf("invalid WAL frame sequence %d",seq)}
    var header [txnWALHeaderBytes]byte
    copy(header[:4],txnWALMagic)
    binary.BigEndian.PutUint16(header[4:6],txnWALFrameVersion)
    binary.BigEndian.PutUint16(header[6:8],txnWALHeaderBytes)
    binary.BigEndian.PutUint64(header[8:16],seq)
    binary.BigEndian.PutUint32(header[16:20],uint32(len(payload)))
    binary.BigEndian.PutUint32(header[20:24],crc32.Checksum(payload,transactionWALCRCTable))
    binary.BigEndian.PutUint32(header[24:28],crc32.Checksum(header[:24],transactionWALCRCTable))
    return append(header[:],payload...),nil
}

// txnWALFrame is a standalone fixture helper; production always allocates
// sequences under the WAL mutex or the checkpoint writer.
func txnWALFrame(rec transactionWALRecord) ([]byte,error) {
    return txnWALFrameWithSequence(rec,1)
}

// readTxnWALFrameAt validates both envelope checksums before decoding an
// indexed frame. Replay additionally validates strictly increasing sequences.
func readTxnWALFrameAt(file *os.File,offset int64)(transactionWALRecord,int64,error){
    var header [txnWALHeaderBytes]byte
    if _,err:=file.ReadAt(header[:],offset);err!=nil{return transactionWALRecord{},0,err}
    n,_,err:=txnWALHeader(header[:])
    if err!=nil{return transactionWALRecord{},0,fmt.Errorf("WAL header at %d: %w",offset,err)}
    buf:=make([]byte,int(n))
    if _,err:=file.ReadAt(buf,offset+txnWALHeaderBytes);err!=nil{return transactionWALRecord{},0,err}
    if crc32.Checksum(buf,transactionWALCRCTable)!=binary.BigEndian.Uint32(header[20:24]){
        return transactionWALRecord{},0,fmt.Errorf("WAL checksum mismatch at %d",offset)
    }
    rec,err:=decodeTxnWALRecord(buf)
    return rec,int64(n)+txnWALHeaderBytes,err
}

func writeTxnWALFrameWithSequence(w io.Writer,rec transactionWALRecord,seq uint64) (int64,error){
    b,err:=txnWALFrameWithSequence(rec,seq)
    if err!=nil{return 0,err}
    n,err:=w.Write(b)
    if err==nil && n!=len(b){err=io.ErrShortWrite}
    return int64(n),err
}
