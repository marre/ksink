package ksink_test

import (
    "bufio"
    "bytes"
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "math/rand"
    "net"
    "os"
    "os/exec"
    "path/filepath"
    "sort"
    "strconv"
    "strings"
    "syscall"
    "testing"
    "time"

    "github.com/marre/ksink/pkg/ksink"
    "github.com/stretchr/testify/require"
    "github.com/twmb/franz-go/pkg/kgo"
)

// All client operations and outcomes are written OUTSIDE the WAL. The checker
// must not infer a successful EndTxn from an unreturned RPC. This is a
// process-level, black-box campaign, unlike in-process WAL fault hooks.
type externalJepsenEvent struct {
    At string `json:"at"`
    Cycle int `json:"cycle"`
    Txn string `json:"txn,omitempty"`
    Operation string `json:"operation"`
    Phase string `json:"phase"`
    Payloads []string `json:"payloads,omitempty"`
    Error string `json:"error,omitempty"`
}

type externalJepsenMessage struct {
    topic string
    data string
    fate string // commit, abort or indeterminate
    ordinal int
}

func externalJepsenCount(t *testing.T,name string,defaultValue int) int {
    t.Helper()
    value:=os.Getenv(name)
    if value=="" {return defaultValue}
    n,err:=strconv.Atoi(value)
    require.NoError(t,err,"%s must be an integer",name)
    require.Greater(t,n,0,"%s must be positive",name)
    return n
}

func externalJepsenOutput(t *testing.T,root,topic string) []string {
    t.Helper()
    dir:=filepath.Join(root,topic)
    entries,err:=os.ReadDir(dir)
    if errors.Is(err,os.ErrNotExist){return nil}
    require.NoError(t,err)
    var names []string
    for _,entry:=range entries {
        if !entry.Type().IsRegular() || !strings.HasPrefix(entry.Name(),"messages-") {
            continue
        }
        names=append(names,entry.Name())
    }
    sort.Strings(names)
    var result []string
    for i,name:=range names {
        require.Equal(t,fmt.Sprintf("messages-%08d.bin",i+1),name,
            "file sequences must never be skipped or reused")
        body,err:=os.ReadFile(filepath.Join(dir,name))
        require.NoError(t,err)
        for _,line:=range strings.Split(string(body),"\n") {
            if line!="" {result=append(result,line)}
        }
    }
    return result
}

func externalJepsenAwaitStart(t *testing.T,bin,addr,walRoot,outputRoot string)(*exec.Cmd,*bytes.Buffer) {
    t.Helper()
    cmd:=exec.Command(bin,
        "--addr",addr,"--transactional",
        "--transaction-state-dir",walRoot,
        "--output",filepath.Join(outputRoot,"{topic}","messages-{seq:08}.bin"),
        "--output-format","binary","--no-separator",
        "--rotate-size","1B","--rotate-interval","200ms",
    )
    logs:=&bytes.Buffer{}
    cmd.Stdout=logs
    cmd.Stderr=logs
    require.NoError(t,cmd.Start())
    deadline:=time.Now().Add(15*time.Second)
    for time.Now().Before(deadline) {
        conn,err:=net.DialTimeout("tcp",addr,100*time.Millisecond)
        if err==nil {
            require.NoError(t,conn.Close())
            return cmd,logs
        }
        time.Sleep(20*time.Millisecond)
    }
    _=cmd.Process.Kill()
    _=cmd.Wait()
    t.Fatalf("external ksink process failed to become ready: %s",logs.String())
    return nil,nil
}

func externalJepsenKill(t *testing.T,cmd *exec.Cmd) {
    t.Helper()
    require.NoError(t,cmd.Process.Kill())
    // SIGKILL is expected. Wait to release OS file locks before restart.
    _=cmd.Wait()
}

func externalJepsenGracefulStop(t *testing.T,cmd *exec.Cmd,logs *bytes.Buffer) {
    t.Helper()
    require.NoError(t,cmd.Process.Signal(syscall.SIGTERM))
    done:=make(chan error,1)
    go func(){done<-cmd.Wait()}()
    select {
    case err:=<-done:
        require.NoError(t,err,"graceful ordered-output flush: %s",logs.String())
    case <-time.After(30*time.Second):
        _=cmd.Process.Kill()
        <-done
        t.Fatalf("external ksink failed to flush before shutdown: %s",logs.String())
    }
}

func TestWALJepsenExternalCampaign(t *testing.T) {
    if testing.Short(){t.Skip("external-process crash campaign")}
    cycles:=externalJepsenCount(t,"KSINK_JEPSEN_CYCLES",3)
    transactions:=externalJepsenCount(t,"KSINK_JEPSEN_TXNS_PER_CYCLE",4)
    seed:=int64(47751)
    if raw:=os.Getenv("KSINK_JEPSEN_SEED");raw!="" {
        parsed,err:=strconv.ParseInt(raw,10,64)
        require.NoError(t,err)
        seed=parsed
    }
    rng:=rand.New(rand.NewSource(seed))
    root:=t.TempDir()
    artifacts:=os.Getenv("KSINK_JEPSEN_ARTIFACT_DIR")
    if artifacts=="" {artifacts=root}
    require.NoError(t,os.MkdirAll(artifacts,0700))
    historyPath:=filepath.Join(artifacts,fmt.Sprintf("wal-jepsen-history-%d.jsonl",seed))
    history,err:=os.OpenFile(historyPath,os.O_CREATE|os.O_EXCL|os.O_WRONLY,0600)
    require.NoError(t,err)
    defer func(){require.NoError(t,history.Close())}()
    encoder:=json.NewEncoder(history)
    emit:=func(event externalJepsenEvent) {
        event.At=time.Now().UTC().Format(time.RFC3339Nano)
        require.NoError(t,encoder.Encode(event))
        require.NoError(t,history.Sync(),"Jepsen history must outlive the WAL process")
    }

    binary:=filepath.Join(root,"ksink")
    build:=exec.Command("go","build","-o",binary,"./cmd/ksink")
    build.Dir=filepath.Join("..","..")
    buildOut,err:=build.CombinedOutput()
    require.NoError(t,err,"compile external ksink server: %s",buildOut)

    walRoot:=filepath.Join(root,"wal")
    outputRoot:=filepath.Join(root,"output")
    require.NoError(t,os.MkdirAll(outputRoot,0700))
    port:=getFreePort(t)
    addr:=fmt.Sprintf("127.0.0.1:%d",port)
    all:=make(map[string]*externalJepsenMessage)
    order:=map[string]int{"orders":0,"payments":0}
    lastNext:=map[string]int64{"orders":0,"payments":0}
    emit(externalJepsenEvent{Operation:"campaign",Phase:"start",
        Payloads:[]string{fmt.Sprintf("seed=%d cycles=%d transactions=%d",seed,cycles,transactions)}})

    for cycle:=0;cycle<cycles;cycle++ {
        cmd,_:=externalJepsenAwaitStart(t,binary,addr,walRoot,outputRoot)
        emit(externalJepsenEvent{Cycle:cycle,Operation:"start",Phase:"ok"})
        if cycle==0 {
            _,err=ksink.OpenTransactionLog(walRoot)
            require.Error(t,err,"a second process must not own the running WAL")
        }
        crashed:=false
        for iteration:=0;iteration<transactions;iteration++ {
            ctx,cancel:=context.WithTimeout(context.Background(),5*time.Second)
            txnID:=fmt.Sprintf("jepsen-producer-%d",iteration%8)
            // Unique payloads allow an external checker to detect duplicates
            // even when a client loses an EndTxn response.
            operationID:=fmt.Sprintf("%d-%d-%d",seed,cycle,iteration)
            records:=[]*kgo.Record{
                {Topic:"orders",Value:[]byte("jepsen-"+operationID+"-orders\n")},
                {Topic:"payments",Value:[]byte("jepsen-"+operationID+"-payments\n")},
            }
            client,clientErr:=kgo.NewClient(
                kgo.SeedBrokers(addr),kgo.TransactionalID(txnID),
                kgo.RequestTimeoutOverhead(800*time.Millisecond),
            )
            require.NoError(t,clientErr)
            require.NoError(t,client.BeginTransaction())
            for _,rec:=range records {
                data:=strings.TrimSuffix(string(rec.Value),"\n")
                all[data]=&externalJepsenMessage{
                    topic:rec.Topic,data:data,fate:"indeterminate",ordinal:order[rec.Topic],
                }
                order[rec.Topic]++
            }
            payloads:=[]string{strings.TrimSuffix(string(records[0].Value),"\n"),
                strings.TrimSuffix(string(records[1].Value),"\n")}
            emit(externalJepsenEvent{Cycle:cycle,Txn:txnID,Operation:"produce",Phase:"invoke",Payloads:payloads})
            results:=client.ProduceSync(ctx,records...)
            if produceErr:=results.FirstErr();produceErr!=nil {
                client.Close()
                cancel()
                externalJepsenKill(t,cmd)
                t.Fatalf("unexpected Kafka Produce error in campaign cycle %d: %v",cycle,produceErr)
            }
            emit(externalJepsenEvent{Cycle:cycle,Txn:txnID,Operation:"produce",Phase:"ok",Payloads:payloads})

            abort:=rng.Intn(5)==0
            action:=kgo.TryCommit
            operation:="commit"
            if abort {action=kgo.TryAbort;operation="abort"}
            emit(externalJepsenEvent{Cycle:cycle,Txn:txnID,Operation:operation,Phase:"invoke",Payloads:payloads})
            // Every other cycle kills the actual process during one EndTxn.
            // Neither a connection error nor timeout is recorded as an abort.
            killDuringEnd:=iteration==transactions-1 && cycle%2==1 && !abort
            if killDuringEnd {
                done:=make(chan error,1)
                go func(){done<-client.EndTransaction(ctx,action)}()
                time.Sleep(time.Duration(rng.Intn(4))*time.Millisecond)
                externalJepsenKill(t,cmd)
                crashed=true
                endErr:=<-done
                event:=externalJepsenEvent{Cycle:cycle,Txn:txnID,Operation:operation,Payloads:payloads}
                if endErr==nil {
                    event.Phase="ok"
                    for _,data:=range payloads {all[data].fate="commit"}
                }else {
                    event.Phase="indeterminate"
                    event.Error=endErr.Error()
                }
                emit(event)
            } else {
                endErr:=client.EndTransaction(ctx,action)
                require.NoError(t,endErr,"EndTxn %s in campaign cycle %d",operation,cycle)
                for _,data:=range payloads {all[data].fate=operation}
                emit(externalJepsenEvent{Cycle:cycle,Txn:txnID,Operation:operation,Phase:"ok",Payloads:payloads})
            }
            client.Close()
            cancel()
            if crashed {break}
        }
        if !crashed {externalJepsenKill(t,cmd)}
        emit(externalJepsenEvent{Cycle:cycle,Operation:"SIGKILL",Phase:"ok"})
        recovered,openErr:=ksink.OpenTransactionLog(walRoot)
        require.NoError(t,openErr,"recover after SIGKILL cycle %d",cycle)
        for _,topic:=range []string{"orders","payments"} {
            offset:=recovered.NextTopicOffset(topic)
            require.GreaterOrEqual(t,offset,lastNext[topic],
                "Kafka offset must never regress after a process restart")
            lastNext[topic]=offset
            require.LessOrEqual(t,recovered.TopicStableOffset(topic),offset)
        }
        require.NoError(t,recovered.Close())
    }

    // Converge without faults, then perform a final graceful flush.
    final,logs:=externalJepsenAwaitStart(t,binary,addr,walRoot,outputRoot)
    externalJepsenGracefulStop(t,final,logs)
    emit(externalJepsenEvent{Operation:"final-flush",Phase:"ok"})
    seen:=make(map[string]int)
    for _,topic:=range []string{"orders","payments"} {
        lastOrdinal:=-1
        for _,payload:=range externalJepsenOutput(t,outputRoot,topic) {
            msg,ok:=all[payload]
            require.True(t,ok,"uninvoked/ghost output payload %q",payload)
            require.Equal(t,topic,msg.topic,"cross-topic output corruption")
            require.NotEqual(t,"abort",msg.fate,"aborted transaction leaked to output")
            seen[payload]++
            require.Equal(t,1,seen[payload],"duplicate immutable output payload")
            require.Greater(t,msg.ordinal,lastOrdinal,"per-topic WAL output order regressed")
            lastOrdinal=msg.ordinal
        }
    }
    confirmed:=0
    ambiguous:=0
    for _,msg:=range all {
        switch msg.fate {
        case "commit":
            require.Equal(t,1,seen[msg.data],"acknowledged committed record missing")
            confirmed++
        case "abort":
            require.Zero(t,seen[msg.data],"aborted payload appeared")
        default:
            require.LessOrEqual(t,seen[msg.data],1,"ambiguous payload duplicated")
            ambiguous++
        }
    }
    emit(externalJepsenEvent{Operation:"verify",Phase:"ok",
        Payloads:[]string{fmt.Sprintf("confirmed=%d ambiguous=%d",confirmed,ambiguous)}})
    t.Logf("external WAL campaign passed: seed=%d cycles=%d transactions/cycle=%d confirmed=%d ambiguous=%d history=%s",
        seed,cycles,transactions,confirmed,ambiguous,historyPath)
}

// Ensure the log is valid newline-delimited JSON independent of WAL files.
func TestWALJepsenExternalHistorySchema(t *testing.T) {
    var event externalJepsenEvent
    require.NoError(t,json.Unmarshal([]byte(`{"at":"2026-01-01T00:00:00Z","cycle":1,"txn":"t","operation":"commit","phase":"indeterminate","payloads":["a"]}`),&event))
    require.Equal(t,"indeterminate",event.Phase)
    scanner:=bufio.NewScanner(strings.NewReader("{}\n"))
    require.True(t,scanner.Scan())
}
