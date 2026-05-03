package ksink

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"net"
	"strconv"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func (s *Server) handleApiVersions(conn net.Conn, connID uint64, correlationID int32, apiVersion int16) error {
	resp := &kmsg.ApiVersionsResponse{
		Version:        apiVersion,
		ErrorCode:      0,
		ThrottleMillis: 0,
	}

	// Always advertise core APIs
	resp.ApiKeys = append(resp.ApiKeys,
		kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.ApiVersions), MinVersion: 0, MaxVersion: 3},
		kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.Metadata), MinVersion: 0, MaxVersion: 12},
		kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.Produce), MinVersion: 0, MaxVersion: 8},
		kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.FindCoordinator), MinVersion: 0, MaxVersion: 5},
	)

	// Only advertise SASL APIs when SASL is enabled
	if s.saslEnabled {
		resp.ApiKeys = append(resp.ApiKeys,
			kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.SASLHandshake), MinVersion: 0, MaxVersion: 1},
			kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.SASLAuthenticate), MinVersion: 0, MaxVersion: 2},
		)
	}

	// Only advertise InitProducerID when IdempotentWrite is enabled
	if s.cfg.IdempotentWrite {
		resp.ApiKeys = append(resp.ApiKeys,
			kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.InitProducerID), MinVersion: 0, MaxVersion: 5},
		)
	}

	// Only advertise transactional APIs when TransactionalWrite is enabled
	if s.cfg.TransactionalWrite {
		resp.ApiKeys = append(resp.ApiKeys,
			kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.AddPartitionsToTxn), MinVersion: 0, MaxVersion: 3},
			kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.EndTxn), MinVersion: 0, MaxVersion: 3},
		)
	}

	s.logger.Debugf("[conn:%d] Sending ApiVersions response with %d keys", connID, len(resp.ApiKeys))

	// ApiVersions always uses non-flexible encoding
	return s.sendResponse(conn, connID, correlationID, resp)
}

func (s *Server) handleMetadata(conn net.Conn, connID uint64, correlationID int32, apiVersion int16, req *kmsg.MetadataRequest) error {
	addr := s.cfg.Address
	if s.cfg.AdvertisedAddress != "" {
		addr = s.cfg.AdvertisedAddress
	}

	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("failed to parse address %s: %w", addr, err)
	}
	port64, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return fmt.Errorf("failed to parse port %s: %w", portStr, err)
	}

	resp := &kmsg.MetadataResponse{
		Version: apiVersion,
		Brokers: []kmsg.MetadataResponseBroker{
			{
				NodeID: 1,
				Host:   host,
				Port:   int32(port64),
			},
		},
		ControllerID: 1,
		ClusterID:    kmsg.StringPtr("ksink-cluster"),
	}

	// Build topic list for response using parsed request topics
	if req.Topics != nil {
		for _, t := range req.Topics {
			topicName := ""
			if t.Topic != nil {
				topicName = *t.Topic
			}
			if s.isTopicAllowed(topicName) {
				resp.Topics = append(resp.Topics, s.buildTopicMetadata(topicName, apiVersion))
			} else {
				resp.Topics = append(resp.Topics, s.buildTopicErrorMetadata(topicName, apiVersion, kerr.UnknownTopicOrPartition))
			}
		}
	} else {
		// nil Topics means "all topics"
		for topic := range s.allowedTopics {
			resp.Topics = append(resp.Topics, s.buildTopicMetadata(topic, apiVersion))
		}
	}

	s.logger.Debugf("[conn:%d] Sending Metadata response: %d brokers, %d topics", connID, len(resp.Brokers), len(resp.Topics))
	return s.sendResponse(conn, connID, correlationID, resp)
}

func (s *Server) buildTopicMetadata(topic string, apiVersion int16) kmsg.MetadataResponseTopic {
	t := kmsg.MetadataResponseTopic{
		Topic: kmsg.StringPtr(topic),
		Partitions: []kmsg.MetadataResponseTopicPartition{
			{
				Partition:   0,
				Leader:      1,
				LeaderEpoch: 1,
				Replicas:    []int32{1},
				ISR:         []int32{1},
			},
		},
	}
	if apiVersion >= 10 {
		t.TopicID = generateTopicID(topic)
	}
	return t
}

func (s *Server) buildTopicErrorMetadata(topic string, apiVersion int16, errCode *kerr.Error) kmsg.MetadataResponseTopic {
	t := kmsg.MetadataResponseTopic{
		Topic:     kmsg.StringPtr(topic),
		ErrorCode: errCode.Code,
	}
	if apiVersion >= 10 {
		t.TopicID = generateTopicID(topic)
	}
	return t
}

func generateTopicID(topic string) [16]byte {
	hash := sha256.Sum256([]byte(topic))
	var id [16]byte
	copy(id[:], hash[:16])
	return id
}

func (s *Server) handleProduce(ctx context.Context, conn net.Conn, connID uint64, correlationID int32, apiVersion int16, req *kmsg.ProduceRequest) error {
	s.logger.Debugf("[conn:%d] Produce request: acks=%d, numTopics=%d", connID, req.Acks, len(req.Topics))

	resp := &kmsg.ProduceResponse{
		Version: apiVersion,
	}

	remoteAddr := conn.RemoteAddr().String()
	txnID := ""
	if req.TransactionID != nil {
		txnID = *req.TransactionID
	}

	// First pass: parse all records and track per-partition status
	type partResult struct {
		errCode int16
	}
	type partKey struct {
		topicIdx, partIdx int
	}
	results := make(map[partKey]partResult)
	var allMsgs []*Message

	for ti, topicData := range req.Topics {
		for pi, partData := range topicData.Partitions {
			key := partKey{ti, pi}

			if !s.isTopicAllowed(topicData.Topic) {
				s.logger.Warnf("[conn:%d] Rejected produce to disallowed topic: %s", connID, topicData.Topic)
				results[key] = partResult{errCode: kerr.UnknownTopicOrPartition.Code}
				continue
			}

			batch, err := s.parseRecords(connID, topicData.Topic, partData.Partition, partData.Records, remoteAddr)
			if err != nil {
				s.logger.Errorf("[conn:%d] Failed to parse records for %s/%d: %v", connID, topicData.Topic, partData.Partition, err)
				results[key] = partResult{errCode: kerr.CorruptMessage.Code}
				continue
			}

			results[key] = partResult{}
			if txnID != "" {
				for _, msg := range batch {
					msg.TransactionalID = txnID
				}
			}
			allMsgs = append(allMsgs, batch...)
		}
	}

	// Deliver batch via channel and wait for ack
	var handlerErr error
	if len(allMsgs) > 0 {
		handlerErr = s.deliverEvent(ctx, &MessagesEvent{Messages: allMsgs})
		if handlerErr != nil {
			s.logger.Errorf("[conn:%d] Batch processing error: %v", connID, handlerErr)
		}
	}

	// Build response
	for ti, topicData := range req.Topics {
		topicResp := kmsg.ProduceResponseTopic{
			Topic: topicData.Topic,
		}

		for pi, partData := range topicData.Partitions {
			partResp := kmsg.ProduceResponseTopicPartition{
				Partition: partData.Partition,
			}

			result := results[partKey{ti, pi}]
			if result.errCode != 0 {
				partResp.ErrorCode = result.errCode
			} else if handlerErr != nil {
				partResp.ErrorCode = kerr.UnknownServerError.Code
			}

			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}

		resp.Topics = append(resp.Topics, topicResp)
	}

	if req.Acks == 0 {
		return nil
	}

	return s.sendResponse(conn, connID, correlationID, resp)
}

// deliverEvent sends an event to Read and waits for the ack.
func (s *Server) deliverEvent(ctx context.Context, evt Event) error {
	pending := pendingBatch{
		event: evt,
		ackCh: make(chan error, 1),
	}
	select {
	case s.batchCh <- pending:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.shutdownCh:
		return ErrServerClosed
	}
	select {
	case err := <-pending.ackCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-s.shutdownCh:
		return ErrServerClosed
	}
}

func (s *Server) handleInitProducerId(ctx context.Context, conn net.Conn, connID uint64, correlationID int32, apiVersion int16, req *kmsg.InitProducerIDRequest, state *connState) error {
	if s.saslEnabled && !state.authenticated {
		s.logger.Warnf("[conn:%d] Rejecting InitProducerId: not authenticated", connID)
		resp := &kmsg.InitProducerIDResponse{
			Version:    apiVersion,
			ErrorCode:  kerr.SaslAuthenticationFailed.Code,
			ProducerID: -1,
		}
		return s.sendResponse(conn, connID, correlationID, resp)
	}

	if !s.cfg.IdempotentWrite {
		s.logger.Warnf("[conn:%d] Rejecting InitProducerId: idempotent_write is disabled", connID)
		resp := &kmsg.InitProducerIDResponse{
			Version:    apiVersion,
			ErrorCode:  kerr.ClusterAuthorizationFailed.Code,
			ProducerID: -1,
		}
		return s.sendResponse(conn, connID, correlationID, resp)
	}

	txnID := ""
	if req.TransactionalID != nil {
		txnID = *req.TransactionalID
	}

	// For transactional producers, track state and implement zombie fencing.
	if txnID != "" && s.cfg.TransactionalWrite {
		s.txnMu.Lock()
		existing, ok := s.txnStates[txnID]
		if ok {
			// Zombie fencing: abort any in-flight transaction from the
			// previous producer instance before bumping the epoch.
			needsAbort := existing.active
			if needsAbort {
				s.logger.Infof("[conn:%d] InitProducerId: fencing zombie txnID=%s (aborting in-flight transaction, epoch %d→%d)",
					connID, txnID, existing.epoch, existing.epoch+1)
				existing.active = false
			}
			if existing.epoch == math.MaxInt16 {
				// int16 would overflow — rotate to a new producer ID
				// and reset the epoch to avoid confusing clients.
				newPID := s.producerIDCounter.Add(1)
				existing.producerID = newPID
				existing.epoch = 0
				s.logger.Warnf("[conn:%d] InitProducerId: epoch overflow for txnID=%s, rotated to new producerID=%d",
					connID, txnID, newPID)
			} else {
				existing.epoch++
			}
			// Copy fields before releasing the lock to avoid a data race.
			producerID := existing.producerID
			epoch := existing.epoch
			s.txnMu.Unlock()

			// Deliver the abort event outside the lock to avoid deadlocks.
			if needsAbort {
				if err := s.deliverEvent(ctx, &TxnAbortEvent{TransactionalID: txnID}); err != nil {
					s.logger.Errorf("[conn:%d] InitProducerId: failed to deliver zombie abort event: %v", connID, err)
				}
			}

			resp := &kmsg.InitProducerIDResponse{
				Version:       apiVersion,
				ProducerID:    producerID,
				ProducerEpoch: epoch,
			}
			s.logger.Debugf("[conn:%d] InitProducerId: txnID=%s, producerID=%d, epoch=%d (existing)",
				connID, txnID, producerID, epoch)
			return s.sendResponse(conn, connID, correlationID, resp)
		}

		// Cap the number of tracked transactional IDs to prevent unbounded
		// memory growth from misbehaving or malicious clients.
		if len(s.txnStates) >= maxTxnStates {
			s.txnMu.Unlock()
			s.logger.Warnf("[conn:%d] Rejecting InitProducerId: too many transactional IDs tracked (%d)", connID, maxTxnStates)
			resp := &kmsg.InitProducerIDResponse{
				Version:    apiVersion,
				ErrorCode:  kerr.UnknownServerError.Code,
				ProducerID: -1,
			}
			return s.sendResponse(conn, connID, correlationID, resp)
		}

		// New transactional ID — assign a fresh producer ID.
		producerID := s.producerIDCounter.Add(1)
		s.txnStates[txnID] = &txnState{
			producerID: producerID,
			epoch:      0,
			active:     false,
		}
		s.txnMu.Unlock()

		resp := &kmsg.InitProducerIDResponse{
			Version:       apiVersion,
			ProducerID:    producerID,
			ProducerEpoch: 0,
		}
		s.logger.Debugf("[conn:%d] InitProducerId: txnID=%s, producerID=%d, epoch=0 (new)",
			connID, txnID, producerID)
		return s.sendResponse(conn, connID, correlationID, resp)
	}

	// Non-transactional idempotent producer — just assign an ID.
	producerID := s.producerIDCounter.Add(1)

	resp := &kmsg.InitProducerIDResponse{
		Version:       apiVersion,
		ProducerID:    producerID,
		ProducerEpoch: 0,
	}

	s.logger.Debugf("[conn:%d] InitProducerId: assigned producerID=%d", connID, producerID)
	return s.sendResponse(conn, connID, correlationID, resp)
}

func (s *Server) handleFindCoordinator(conn net.Conn, connID uint64, correlationID int32, apiVersion int16, req *kmsg.FindCoordinatorRequest) error {
	addr := s.cfg.Address
	if s.cfg.AdvertisedAddress != "" {
		addr = s.cfg.AdvertisedAddress
	}

	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("failed to parse address %s: %w", addr, err)
	}
	parsedPort, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return fmt.Errorf("failed to parse port %s: %w", portStr, err)
	}
	port := int32(uint16(parsedPort))

	resp := &kmsg.FindCoordinatorResponse{
		Version:        apiVersion,
		NodeID:         1,
		Host:           host,
		Port:           port,
		ErrorCode:      0,
		ThrottleMillis: 0,
	}

	// For version >= 4, use Coordinators array
	if apiVersion >= 4 {
		var keys []string
		if len(req.CoordinatorKeys) > 0 {
			keys = req.CoordinatorKeys
		} else {
			keys = []string{req.CoordinatorKey}
		}
		for _, key := range keys {
			resp.Coordinators = append(resp.Coordinators, kmsg.FindCoordinatorResponseCoordinator{
				Key:    key,
				NodeID: 1,
				Host:   host,
				Port:   port,
			})
		}
	}

	s.logger.Debugf("[conn:%d] FindCoordinator response: host=%s, port=%d", connID, host, port)
	return s.sendResponse(conn, connID, correlationID, resp)
}

func (s *Server) handleAddPartitionsToTxn(conn net.Conn, connID uint64, correlationID int32, apiVersion int16, req *kmsg.AddPartitionsToTxnRequest, state *connState) error {
	if s.saslEnabled && !state.authenticated {
		s.logger.Warnf("[conn:%d] Rejecting AddPartitionsToTxn: not authenticated", connID)
		resp := &kmsg.AddPartitionsToTxnResponse{
			Version:   apiVersion,
			ErrorCode: kerr.SaslAuthenticationFailed.Code,
		}
		return s.sendResponse(conn, connID, correlationID, resp)
	}

	if !s.cfg.TransactionalWrite {
		s.logger.Warnf("[conn:%d] Rejecting AddPartitionsToTxn: transactional_write is disabled", connID)
		resp := &kmsg.AddPartitionsToTxnResponse{
			Version:   apiVersion,
			ErrorCode: kerr.TransactionalIDAuthorizationFailed.Code,
		}
		return s.sendResponse(conn, connID, correlationID, resp)
	}

	resp := &kmsg.AddPartitionsToTxnResponse{
		Version: apiVersion,
	}

	for _, topic := range req.Topics {
		topicResp := kmsg.AddPartitionsToTxnResponseTopic{
			Topic: topic.Topic,
		}
		for _, partition := range topic.Partitions {
			topicResp.Partitions = append(topicResp.Partitions, kmsg.AddPartitionsToTxnResponseTopicPartition{
				Partition: partition,
				ErrorCode: 0,
			})
		}
		resp.Topics = append(resp.Topics, topicResp)
	}

	// Validate producerID/epoch against txnState and mark the transaction as active.
	s.txnMu.Lock()
	st, stOK := s.txnStates[req.TransactionalID]
	if !stOK || st.producerID != req.ProducerID || st.epoch != req.ProducerEpoch {
		s.txnMu.Unlock()
		// Return ProducerFenced for all partitions.
		for ti := range resp.Topics {
			for pi := range resp.Topics[ti].Partitions {
				resp.Topics[ti].Partitions[pi].ErrorCode = kerr.ProducerFenced.Code
			}
		}
		s.logger.Warnf("[conn:%d] AddPartitionsToTxn fenced: txnID=%s, producerID=%d, epoch=%d",
			connID, req.TransactionalID, req.ProducerID, req.ProducerEpoch)
		return s.sendResponse(conn, connID, correlationID, resp)
	}
	st.active = true
	s.txnMu.Unlock()

	s.logger.Debugf("[conn:%d] AddPartitionsToTxn: txnID=%s, topics=%d", connID, req.TransactionalID, len(req.Topics))
	return s.sendResponse(conn, connID, correlationID, resp)
}

func (s *Server) handleEndTxn(ctx context.Context, conn net.Conn, connID uint64, correlationID int32, apiVersion int16, req *kmsg.EndTxnRequest, state *connState) error {
	if s.saslEnabled && !state.authenticated {
		s.logger.Warnf("[conn:%d] Rejecting EndTxn: not authenticated", connID)
		resp := &kmsg.EndTxnResponse{
			Version:   apiVersion,
			ErrorCode: kerr.SaslAuthenticationFailed.Code,
		}
		return s.sendResponse(conn, connID, correlationID, resp)
	}

	if !s.cfg.TransactionalWrite {
		s.logger.Warnf("[conn:%d] Rejecting EndTxn: transactional_write is disabled", connID)
		resp := &kmsg.EndTxnResponse{
			Version:   apiVersion,
			ErrorCode: kerr.TransactionalIDAuthorizationFailed.Code,
		}
		return s.sendResponse(conn, connID, correlationID, resp)
	}

	action := "commit"
	if !req.Commit {
		action = "abort"
	}

	resp := &kmsg.EndTxnResponse{
		Version: apiVersion,
	}

	// Validate producerID/epoch against txnState before allowing the commit/abort.
	fenced := false
	s.txnMu.Lock()
	if st, ok := s.txnStates[req.TransactionalID]; ok {
		if st.producerID != req.ProducerID || st.epoch != req.ProducerEpoch {
			fenced = true
		} else {
			st.active = false
		}
	} else {
		fenced = true
	}
	s.txnMu.Unlock()

	if fenced {
		resp.ErrorCode = kerr.ProducerFenced.Code
		s.logger.Warnf("[conn:%d] EndTxn fenced: txnID=%s, producerID=%d, epoch=%d",
			connID, req.TransactionalID, req.ProducerID, req.ProducerEpoch)
		return s.sendResponse(conn, connID, correlationID, resp)
	}

	// Deliver a transaction-end event through Read so the consumer
	// can handle commit/abort as part of the pull-based flow.
	var evt Event
	if req.Commit {
		evt = &TxnCommitEvent{TransactionalID: req.TransactionalID}
	} else {
		evt = &TxnAbortEvent{TransactionalID: req.TransactionalID}
	}
	if err := s.deliverEvent(ctx, evt); err != nil {
		s.logger.Errorf("[conn:%d] EndTxn: failed to deliver txn event: %v", connID, err)
		resp.ErrorCode = kerr.UnknownServerError.Code
		return s.sendResponse(conn, connID, correlationID, resp)
	}

	s.logger.Debugf("[conn:%d] EndTxn: txnID=%s, action=%s", connID, req.TransactionalID, action)
	return s.sendResponse(conn, connID, correlationID, resp)
}

func (s *Server) isTopicAllowed(topic string) bool {
	if len(s.allowedTopics) == 0 {
		return true
	}
	_, ok := s.allowedTopics[topic]
	return ok
}

func (s *Server) parseRecords(connID uint64, topic string, partition int32, data []byte, remoteAddr string) ([]*Message, error) {
	if len(data) == 0 {
		return nil, nil
	}

	// Create a fake FetchPartition to use kgo's record parsing
	fakePartition := kmsg.FetchResponseTopicPartition{
		Partition:     partition,
		RecordBatches: data,
	}

	opts := kgo.ProcessFetchPartitionOpts{
		Topic:                topic,
		Partition:            partition,
		KeepControlRecords:   false,
		DisableCRCValidation: true,
	}

	fp, _ := kgo.ProcessFetchPartition(opts, &fakePartition, s.decompressor, nil)

	if fp.Err != nil {
		return nil, fmt.Errorf("failed to process records: %w", fp.Err)
	}

	msgs := make([]*Message, 0, len(fp.Records))
	for _, record := range fp.Records {
		msg := &Message{
			Topic:      record.Topic,
			Partition:  record.Partition,
			Offset:     record.Offset,
			Key:        record.Key,
			Value:      record.Value,
			Timestamp:  record.Timestamp,
			Tombstone:  record.Value == nil,
			ClientAddr: remoteAddr,
			Headers:    make(map[string]string),
		}

		for _, header := range record.Headers {
			msg.Headers[header.Key] = string(header.Value)
		}

		msgs = append(msgs, msg)
	}

	s.logger.Debugf("[conn:%d] Parsed %d records", connID, len(msgs))
	return msgs, nil
}
