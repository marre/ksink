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
func runOrdered(addr,dst,stateDir string, fmtr format.Formatter,
	rotateSize int64,rotateInterval time.Duration) error {
	if dst=="-" || strings.Contains(dst,"://") {
		return errors.New("--transactional requires a filesystem path with {topic} and {seq:NN}")
	}
	if stateDir=="" {stateDir=output.KafkaTransactionStateDir(dst)}
	wal,err:=ksink.OpenTransactionLog(stateDir)
	if err!=nil{return fmt.Errorf("open ordered transaction WAL: %w",err)}
	defer wal.Close() //nolint:errcheck

	writer,err:=output.NewOrderedTopicMaterializer(wal,dst,rotateSize,rotateInterval)
	if err!=nil{return fmt.Errorf("configure ordered output writer: %w",err)}
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
	select {
	case <-signals:
		log.Print("Shutting down ordered Kafka server")
	case writerErr=<-errCh:
		if writerErr!=nil {log.Printf("Ordered output materializer failed: %v",writerErr)}
	}
	if err:=srv.Close(context.Background());err!=nil && writerErr==nil {
		writerErr=fmt.Errorf("close Kafka server: %w",err)
	}
	workerCancel()
	// The materializer goroutine can still hold its output lock until its
	// last publish call returns. Flush serializes behind it.
	if err:=writer.Flush(context.Background());err!=nil && writerErr==nil {
		writerErr=fmt.Errorf("flush ordered output: %w",err)
	}
	return writerErr
}
