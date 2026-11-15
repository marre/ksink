package ksink_test

import (
    "context"
    "encoding/json"
    "fmt"
    "os"
    "os/exec"
    "path/filepath"
    "strconv"
    "strings"
    "sync"
    "testing"
    "time"

    "github.com/marre/ksink/pkg/ksink"
    "github.com/stretchr/testify/require"
    "github.com/twmb/franz-go/pkg/kgo"
)

// Exercises five overlapping transactional clients, each with multi-topic
// Produce, racing commits and aborts, process crashes between rounds and
// recovery. The external oracle uses actual Kafka-assigned offsets, not
// invocation order (which is intentionally nondeterministic here).
func TestWALFaultExternalConcurrentTransactions(t *testing.T) {
    if testing.Short() {t.Skip("external-process concurrency fault campaign")}
    cycles:=externalFaultCount(t,"KSINK_FAULT_PARALLEL_CYCLES",2)
    const producers=5
    root:=t.TempDir()
    bin:=filepath.Join(root,"ksink")
    build:=exec.Command("go","build","-o",bin,"./cmd/ksink")
    build.Dir=filepath.Join("..","..")
    out,err:=build.CombinedOutput()
    require.NoError(t,err,"build real ksink executable: %s",out)

    artifacts:=os.Getenv("KSINK_FAULT_ARTIFACT_DIR")
    if artifacts=="" {artifacts=root}
    require.NoError(t,os.MkdirAll(artifacts,0700))
    history,err:=os.OpenFile(filepath.Join(artifacts,"wal-fault-concurrent-history.jsonl"),
        os.O_CREATE|os.O_EXCL|os.O_WRONLY,0600)
    require.NoError(t,err)
    defer func(){require.NoError(t,history.Close())}()
    encoder:=json.NewEncoder(history)
    emit:=func(cycle int,result concurrentFaultResult){
        require.NoError(t,encoder.Encode(map[string]any{
            "cycle":cycle,"operation":"end-txn","client":result.client,
            "phase":"ok","decision":result.decision,
            "orders":result.orderPayload,"payments":result.paymentPayload,
            "order_offset":result.orderOffset,"payment_offset":result.paymentOffset,
            "timestamp":time.Now().UTC().Format(time.RFC3339Nano),
        }))
        require.NoError(t,history.Sync())
    }
    walRoot:=filepath.Join(root,"wal")
    outputRoot:=filepath.Join(root,"output")
    require.NoError(t,os.MkdirAll(outputRoot,0700))
    addr:=fmt.Sprintf("127.0.0.1:%d",getFreePort(t))
    // All topics have one partition; offsets are authoritative across
    // simultaneous Kafka producer invocations, aborts and control markers.
    expected:=map[string]map[string]int64{
        "orders":{},"payments":{},
    }
    forbidden:=map[string]bool{}
    lastOffsets:=map[string]int64{}
    for cycle:=0;cycle<cycles;cycle++ {
        process,_:=externalFaultAwaitStart(t,bin,addr,walRoot,outputRoot)
        start:=make(chan struct{})
        results:=make([]concurrentFaultResult,producers)
        var wg sync.WaitGroup
        for i:=range results {
            wg.Add(1)
            go func(i int) {
                defer wg.Done()
                <-start
                results[i]=runConcurrentFaultTxn(addr,cycle,i)
            }(i)
        }
        close(start)
        wg.Wait()
        for _,r:=range results {
            require.NoError(t,r.err,"concurrent client %d cycle %d",r.client,cycle)
            emit(cycle,r)
            if r.decision=="commit" {
                expected["orders"][r.orderPayload]=r.orderOffset
                expected["payments"][r.paymentPayload]=r.paymentOffset
            } else {
                forbidden[r.orderPayload]=true
                forbidden[r.paymentPayload]=true
            }
        }
        externalFaultKill(t,process)
        recovered,err:=ksink.OpenTransactionLog(walRoot)
        require.NoError(t,err,"parallel producer recovery in cycle %d",cycle)
        for _,topic:=range []string{"orders","payments"} {
            next:=recovered.NextTopicOffset(topic)
            require.GreaterOrEqual(t,next,lastOffsets[topic])
            lastOffsets[topic]=next
        }
        require.NoError(t,recovered.Close())
    }
    final,logs:=externalFaultAwaitStart(t,bin,addr,walRoot,outputRoot)
    externalFaultGracefulStop(t,final,logs)
    for _,topic:=range []string{"orders","payments"} {
        outputs:=externalFaultOutput(t,outputRoot,topic)
        seen:=make(map[string]bool)
        prev:=int64(-1)
        for _,payload:=range outputs {
            require.False(t,seen[payload],"duplicate committed output %s",payload)
            seen[payload]=true
            require.False(t,forbidden[payload],"aborted payload leaked %s",payload)
            offset,ok:=expected[topic][payload]
            require.True(t,ok,"unexpected output %s",payload)
            require.Greater(t,offset,prev,"committed output must follow Kafka-offset order")
            prev=offset
        }
        require.Len(t,outputs,len(expected[topic]),"lost committed output on %s",topic)
        for payload:=range expected[topic] {require.True(t,seen[payload],"missing committed payload %s",payload)}
    }
}

type concurrentFaultResult struct {
    client int
    decision string
    orderPayload string
    paymentPayload string
    orderOffset int64
    paymentOffset int64
    err error
}

func runConcurrentFaultTxn(addr string,cycle,clientID int) concurrentFaultResult {
    result:=concurrentFaultResult{client:clientID}
    result.orderPayload="parallel-"+strconv.Itoa(cycle)+"-"+strconv.Itoa(clientID)+"-orders"
    result.paymentPayload="parallel-"+strconv.Itoa(cycle)+"-"+strconv.Itoa(clientID)+"-payments"
    ctx,cancel:=context.WithTimeout(context.Background(),15*time.Second)
    defer cancel()
    client,err:=kgo.NewClient(
        kgo.SeedBrokers(addr),kgo.TransactionalID(fmt.Sprintf("parallel-%d",clientID)),
        kgo.RequestTimeoutOverhead(time.Second),
    )
    if err!=nil {result.err=err;return result}
    defer client.Close()
    if err=client.BeginTransaction();err!=nil {result.err=err;return result}
    outputs:=client.ProduceSync(ctx,
        &kgo.Record{Topic:"orders",Value:[]byte(result.orderPayload+"\n")},
        &kgo.Record{Topic:"payments",Value:[]byte(result.paymentPayload+"\n")},
    )
    if err=outputs.FirstErr();err!=nil {result.err=err;return result}
    if len(outputs)!=2 {
        result.err=fmt.Errorf("expected two Produce records, received %d",len(outputs))
        return result
    }
    for _,item:=range outputs {
        switch item.Record.Topic {
        case "orders": result.orderOffset=item.Record.Offset
        case "payments": result.paymentOffset=item.Record.Offset
        default: result.err=fmt.Errorf("unexpected produced topic %q",item.Record.Topic);return result
        }
    }
    action:=kgo.TryCommit
    result.decision="commit"
    if (cycle+clientID)%4==0 {
        action=kgo.TryAbort
        result.decision="abort"
    }
    if err=client.EndTransaction(ctx,action);err!=nil {
        result.err=fmt.Errorf("%s concurrent transaction: %w",result.decision,err)
        return result
    }
    result.orderPayload=strings.TrimSuffix(result.orderPayload,"\n")
    return result
}
