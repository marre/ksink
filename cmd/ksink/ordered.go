package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/marre/ksink/internal/format"
	"github.com/marre/ksink/internal/output"
	"github.com/marre/ksink/pkg/ksink"
)

// parseRotationSize accepts decimal bytes or an integral binary/decimal
// suffix. No third-party parser is needed for these few explicit units.
func parseRotationSize(value string) (int64,error) {
	v:=strings.TrimSpace(value)
	if v=="" {return 0,errors.New("empty rotation size")}
	multipliers:=[]struct{suffix string; value int64}{
		{"GiB",1<<30},{"MiB",1<<20},{"KiB",1<<10},
		{"GB",1000000000},{"MB",1000000},{"KB",1000},{"B",1},
	}
	mult:=int64(1)
	for _,u:=range multipliers {
		if strings.HasSuffix(v,u.suffix){
			v=strings.TrimSpace(strings.TrimSuffix(v,u.suffix))
			mult=u.value
			break
		}
	}
	if v=="" {return 0,fmt.Errorf("invalid rotation size %q",value)}
	n,err:=strconv.ParseInt(v,10,64)
	if err!=nil || n<0 || n>int64(^uint64(0)>>1)/mult {
		return 0,fmt.Errorf("invalid or overflowing rotation size %q",value)
	}
	return n*mult,nil
}

// runOrdered starts a single shared transactional WAL and a single filesystem
// materializer that independently publishes completed files for each topic.
// Kafka successes are based on WAL durability, not asynchronous publication.
func runOrdered(addr,dst,stateDir string, fmtr format.Formatter, serialization string,
	rotateSize int64,rotateInterval time.Duration) error {
	return runOrderedWithS3(addr,dst,stateDir,fmtr,serialization,rotateSize,rotateInterval,output.S3OrderedOpts{})
}

// runOrderedWithS3 shares the same WAL and Kafka acknowledgement boundary for
// filesystem and S3 destinations. S3 is a publication backend, not a Kafka
// transactional writer.
func runOrderedWithS3(addr,dst,stateDir string, fmtr format.Formatter, serialization string,
	rotateSize int64,rotateInterval time.Duration,s3Opts output.S3OrderedOpts) error {
	isS3:=strings.HasPrefix(dst,"s3://")
	if dst=="-" || (!isS3 && strings.Contains(dst,"://")) {
		return errors.New("--transactional requires a filesystem path or s3://bucket/prefix/{topic}/file-{seq:NN}")
	}
	if isS3 && stateDir=="" {
		return errors.New("S3 ordered output requires explicit --transaction-state-dir for its authoritative local WAL")
	}
	if stateDir=="" {stateDir=output.KafkaTransactionStateDir(dst)}
	wal,err:=ksink.OpenTransactionLog(stateDir)
	if err!=nil{return fmt.Errorf("open ordered transaction WAL: %w",err)}
	defer wal.Close() //nolint:errcheck

	var writer *output.OrderedTopicMaterializer
	if isS3 {
		writer,err=output.NewS3OrderedTopicMaterializer(wal,dst,s3Opts,rotateSize,rotateInterval)
	} else {
		writer,err=output.NewOrderedTopicMaterializer(wal,dst,rotateSize,rotateInterval)
	}
	if err!=nil{return fmt.Errorf("configure ordered output writer: %w",err)}
	if err:=wal.BindFormatter(serialization);err!=nil {
		return fmt.Errorf("verify ordered output serialization configuration: %w",err)
	}
	srv,err:=ksink.New(ksink.Config{
		Address:addr,TransactionalWrite:true,
	},ksink.WithLogger(stdLogger{}),ksink.WithTransactionLog(wal),
		ksink.WithOrderedTransactionalOutput(fmtr.Format))
	if err!=nil{return fmt.Errorf("create ordered Kafka server: %w",err)}
	if err:=srv.Start(context.Background());err!=nil{return fmt.Errorf("start Kafka server: %w",err)}
	log.Printf("Kafka server listening on %s, ordered committed output=%s, rotate-size=%d, rotate-interval=%s",
		srv.Addr(),dst,rotateSize,rotateInterval)

	workerCtx,workerCancel:=context.WithCancel(context.Background())
	errCh:=make(chan error,1)
	go func(){errCh<-writer.Run(workerCtx)}()
	signals:=make(chan os.Signal,1)
	signal.Notify(signals,syscall.SIGINT,syscall.SIGTERM)
	defer signal.Stop(signals)

	var writerErr error
	workerDone:=false
	select {
	case <-signals:
		log.Print("Shutting down ordered Kafka server")
	case writerErr=<-errCh:
		workerDone=true
		if writerErr!=nil {log.Printf("Ordered output materializer failed: %v",writerErr)}
	}
	if err:=srv.Close(context.Background());err!=nil && writerErr==nil {
		writerErr=fmt.Errorf("close Kafka server: %w",err)
	}
	workerCancel()
	// Wait for the worker to release the publication lock before flushing.
	// Otherwise an in-flight retry could race shutdown diagnostics.
	if !workerDone {
		if err:=<-errCh;err!=nil && writerErr==nil {writerErr=fmt.Errorf("ordered output worker: %w",err)}
	}
	// On a fatal publication/WAL failure, do not attempt further writes.
	// The next process will reconcile durable WAL records and final files.
	if writerErr==nil {
		if err:=writer.Flush(context.Background());err!=nil {
			writerErr=fmt.Errorf("flush ordered output: %w",err)
		}
	}
	return writerErr
}
