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
	walOutputHeaderBytes = 1 + 4 + 2 + 4 + 4
)

// encodeTxnWALRecord stores output bytes without JSON/base64 expansion.
// Metadata records remain small, versioned JSON documents. The outer 8-byte
// length and CRC32C envelope is identical for both types of record.
func encodeTxnWALRecord(rec transactionWALRecord) ([]byte, error) {
	rec.Version = transactionWALVersion
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

func txnWALFrame(rec transactionWALRecord) ([]byte,error) {
	payload,err:=encodeTxnWALRecord(rec)
	if err!=nil {return nil,err}
	var header [8]byte
	binary.BigEndian.PutUint32(header[:4],uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:],crc32.Checksum(payload,transactionWALCRCTable))
	return append(header[:],payload...),nil
}

// readTxnWALFrameAt verifies the checksum and structured representation,
// ensuring that indexed payload streaming is also protected against bit rot.
func readTxnWALFrameAt(file *os.File,offset int64)(transactionWALRecord,int64,error){
	var header [8]byte
	if _,err:=file.ReadAt(header[:],offset);err!=nil{return transactionWALRecord{},0,err}
	n:=binary.BigEndian.Uint32(header[:4])
	if n==0 || n>transactionWALMaxFrame {return transactionWALRecord{},0,fmt.Errorf("invalid WAL record length %d",n)}
	buf:=make([]byte,int(n))
	if _,err:=file.ReadAt(buf,offset+8);err!=nil{return transactionWALRecord{},0,err}
	if crc32.Checksum(buf,transactionWALCRCTable)!=binary.BigEndian.Uint32(header[4:]){
		return transactionWALRecord{},0,fmt.Errorf("WAL checksum mismatch at %d",offset)
	}
	rec,err:=decodeTxnWALRecord(buf)
	return rec,int64(n)+8,err
}

func writeTxnWALFrame(w io.Writer, rec transactionWALRecord) (int64,error){
	b,err:=txnWALFrame(rec)
	if err!=nil{return 0,err}
	n,err:=w.Write(b)
	if err==nil && n!=len(b){err=io.ErrShortWrite}
	return int64(n),err
}
