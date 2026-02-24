package ksrv_test

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/marre/ksrv"
)

func TestIntegrationKafkaServerPerfTest(t *testing.T) {
	checkIntegrationSkip(t)
	t.Parallel()

	const numRecords = 10000
	const recordSizeBytes = 100

	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "perf_output.txt")

	opts := ksrv.Options{
		Address:           fmt.Sprintf("0.0.0.0:%d", port),
		AdvertisedAddress: hostAddr,
		Timeout:           30 * time.Second,
	}

	srv, err := ksrv.New(opts)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	require.NoError(t, srv.Connect(ctx))

	f, err := os.Create(outputFile)
	require.NoError(t, err)
	defer f.Close()

	capture := &messageCapture{}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			batch, ackFn, err := srv.ReadBatch(ctx)
			if err != nil {
				return
			}
			for _, msg := range batch {
				_, _ = f.Write(msg.Value)
				_, _ = f.WriteString("\n")

				rm := receivedMessage{Topic: msg.Topic, Value: string(msg.Value)}
				capture.add(rm)
			}
			_ = ackFn(ctx, nil)
		}
	}()

	waitForTCPReady(t, fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)

	pool := newDockerPool(t)
	client := newDockerExecClient(t, pool, dockerContainerOpts{
		repository: "apache/kafka",
		tag:        "4.1.1",
		cmd:        []string{"sleep", "infinity"},
		readyCmd:   []string{"echo", "ready"},
	})
	t.Cleanup(func() { client.Close() })

	cmd := []string{
		"/opt/kafka/bin/kafka-producer-perf-test.sh",
		"--topic", "perf-test-topic",
		"--num-records", fmt.Sprintf("%d", numRecords),
		"--record-size", fmt.Sprintf("%d", recordSizeBytes),
		"--throughput", "-1",
		"--producer-props",
		fmt.Sprintf("bootstrap.servers=%s", hostAddr),
		"acks=1",
		"retries=0",
		"delivery.timeout.ms=30000",
		"request.timeout.ms=5000",
	}

	t.Logf("Running kafka-producer-perf-test.sh: %v", cmd)
	stdout, stderr, err := client.exec(cmd)
	t.Logf("kafka-producer-perf-test.sh stdout:\n%s", stdout)
	if stderr != "" {
		t.Logf("kafka-producer-perf-test.sh stderr:\n%s", stderr)
	}
	require.NoError(t, err, "kafka-producer-perf-test.sh failed")

	require.True(t, capture.waitForCount(numRecords, 60*time.Second),
		"expected all %d records to arrive", numRecords)

	t.Logf("Successfully received all %d messages", numRecords)

	cancel()
	_ = srv.Close(context.Background())
	wg.Wait()

	// Reopen file to count lines
	rf, err := os.Open(outputFile)
	require.NoError(t, err)
	defer rf.Close()

	lineCount := 0
	scanner := bufio.NewScanner(rf)
	for scanner.Scan() {
		lineCount++
	}
	require.NoError(t, scanner.Err())
	require.Equal(t, numRecords, lineCount, "expected %d lines in output file, got %d", numRecords, lineCount)
	t.Logf("Output file contains %d lines", lineCount)
}
