package ksink

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"sort"
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
		ClusterID:    new("ksink-cluster"),
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

// ksink intentionally exposes exactly one partition (partition 0) for each
// topic. Do not accept direct Produce or AddPartitionsToTxn requests for
// partitions that the metadata endpoint does not advertise.
const onlyPartition int32 = 0

func (s *Server) buildTopicMetadata(topic string, apiVersion int16) kmsg.MetadataResponseTopic {
	t := kmsg.MetadataResponseTopic{
		Topic: new(topic),
		Partitions: []kmsg.MetadataResponseTopicPartition{
			{
				Partition:   onlyPartition,
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
		Topic:     new(topic),
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

	// For transactional produce, verify the active transaction and producer
	// epoch. The epoch lives in each record-batch header, not in ProduceRequest.
	var txnProduceErr int16
	var txnProducerID int64
	var txnProducerEpoch int16
	var txnInstance string
	var txnProduceState *txnState
	if txnID != "" && s.cfg.TransactionalWrite {
		s.txnMu.Lock()
		st, ok := s.txnStates[txnID]
		switch {
		case !ok:
			txnProduceErr = kerr.ProducerFenced.Code
		case st.restartRequired:
			txnProduceErr = kerr.ProducerFenced.Code
		case !st.active || st.ending || st.needsResolution || s.txnStoreErr != nil:
			txnProduceErr = kerr.InvalidTxnState.Code
		default:
			txnProducerID, txnProducerEpoch = st.producerID, st.epoch
			txnInstance = st.instance
			// Adding while holding txnMu is paired with EndTxn marking the
			// state as ending under the same mutex before waiting.
			st.inFlight.Add(1)
			txnProduceState = st
		}
		s.txnMu.Unlock()
	}
	var proposedSequences map[string]txnPartitionSequence
	var sequenceChanged bool
	if txnProduceState != nil {
		// Transaction batches may arrive over multiple connections. Serialize
		// the dedup check, sink delivery, and durable sequence checkpoint so
		// two concurrent retries cannot both deliver the same batch.
		txnProduceState.produceMu.Lock()
		defer txnProduceState.produceMu.Unlock()
		defer txnProduceState.inFlight.Done()
		proposedSequences = copyTxnSequences(txnProduceState.sequences)
	}

	// First pass: parse all records and track per-partition status
	type partResult struct {
		errCode int16
		first int
		count int
		accepted []byte
		baseOffset int64
	}
	type partKey struct {
		topicIdx, partIdx int
	}
	results := make(map[partKey]partResult)
	var allMsgs []*Message

	for ti, topicData := range req.Topics {
		for pi, partData := range topicData.Partitions {
			key := partKey{ti, pi}

			if !s.isTopicAllowed(topicData.Topic) || partData.Partition != onlyPartition {
				s.logger.Warnf("[conn:%d] Rejected produce to unsupported topic-partition: %s/%d", connID, topicData.Topic, partData.Partition)
				results[key] = partResult{errCode: kerr.UnknownTopicOrPartition.Code}
				continue
			}
			if txnProduceErr != 0 {
				results[key] = partResult{errCode: txnProduceErr}
				continue
			}
			// Kafka requires enrollment before writing transaction records.
			// Only the new ordered WAL path enforces this so the existing
			// legacy filesystem backend remains unchanged until cutover.
			if s.orderedFormatter != nil && txnProduceState != nil {
				found := false
				for _, name := range txnProduceState.topics {
					if name == topicData.Topic { found = true; break }
				}
				if !found {
					results[key] = partResult{errCode: kerr.InvalidTxnState.Code}
					continue
				}
			}
			recordBatches := partData.Records
			var candidate map[string]txnPartitionSequence
			if txnID != "" && s.cfg.TransactionalWrite {
				if code := validateTxnBatchProducer(recordBatches, txnProducerID, txnProducerEpoch); code != 0 {
					results[key] = partResult{errCode: code}
					continue
				}
				// Don't advance any sequence for batches that fail decoding.
				candidate = copyTxnSequences(proposedSequences)
				var code int16
				recordBatches, code = acceptTxnBatches(recordBatches,
					txnSequenceKey(topicData.Topic, partData.Partition), candidate)
				if code != 0 {
					results[key] = partResult{errCode: code}
					continue
				}
				if len(recordBatches) == 0 {
					// An exact retransmission must keep its ORIGINAL Kafka
					// base offset, not return a new append position.
					result := partResult{baseOffset: -1}
					if s.orderedFormatter != nil {
						if offset, ok := txnBatchFirstOffset(partData.Records,
							txnProduceState.sequences[txnSequenceKey(topicData.Topic, partData.Partition)]); ok {
							result.baseOffset = offset
						}
					}
					results[key] = result
					continue
				}
			}

			batch, err := s.parseRecords(connID, topicData.Topic, partData.Partition, recordBatches, remoteAddr)
			if err != nil {
				s.logger.Errorf("[conn:%d] Failed to parse records for %s/%d: %v", connID, topicData.Topic, partData.Partition, err)
				results[key] = partResult{errCode: kerr.CorruptMessage.Code}
				continue
			}

			result := partResult{first: len(allMsgs), count: len(batch),
				accepted: recordBatches, baseOffset: -1}
			if s.orderedFormatter != nil && txnProduceState != nil {
				if offset, ok := txnBatchFirstOffset(partData.Records,
					txnProduceState.sequences[txnSequenceKey(topicData.Topic, partData.Partition)]); ok {
					result.baseOffset = offset
				}
			}
			results[key] = result
			if txnProduceState != nil && candidate != nil {
				proposedSequences = candidate
				sequenceChanged = true
			}
			if txnID != "" {
				for _, msg := range batch {
					msg.TransactionalID = txnID
					msg.TransactionInstance = txnInstance
				}
			}
			allMsgs = append(allMsgs, batch...)
		}
	}

	// In ordered-output mode, the WAL itself is the durable sink. It must
	// assign offsets BEFORE the formatter runs and must checkpoint producer
	// sequences inside the SAME acceptance frame as the payloads.
	var handlerErr error
	if len(allMsgs) > 0 {
		if s.orderedFormatter != nil {
			records := make([]WALTopicRecord, 0, len(allMsgs))
			for _, msg := range allMsgs {
				records = append(records, WALTopicRecord{
					Topic: msg.Topic, TransactionID: msg.TransactionalID,
					Instance: msg.TransactionInstance,
				})
			}
			encode := func(i int, offset int64) ([]byte, error) {
				allMsgs[i].Offset = offset
				return s.orderedFormatter(allMsgs[i])
			}
			var offsets []int64
			if txnProduceState != nil {
				// Serialize state snapshot preparation with all producer
				// generations while WAL assigns offsets and fsyncs.
				s.txnMu.Lock()
				if s.txnStoreErr != nil {
					handlerErr = s.txnStoreErr
				} else {
					var updatedSequences map[string]txnPartitionSequence
					offsets, handlerErr = s.txnLog.appendTopicBatchFormattedWithStateBuilder(
						records, encode, func(assigned []int64) (txnStateSnapshot, error) {
							updatedSequences = copyTxnSequences(proposedSequences)
							for ti, topicData := range req.Topics {
								for pi, partData := range topicData.Partitions {
									result := results[partKey{ti, pi}]
									if result.count == 0 { continue }
									sequenceKey := txnSequenceKey(topicData.Topic, partData.Partition)
									seq := updatedSequences[sequenceKey]
									if err := assignTxnAcceptedOffsets(result.accepted,
										result.first, assigned, &seq); err != nil {
										return txnStateSnapshot{}, err
									}
									updatedSequences[sequenceKey] = seq
								}
							}
							snapshot := s.txnSnapshotLocked()
							current := snapshot.States[txnID]
							current.Sequences = updatedSequences
							snapshot.States[txnID] = current
							return snapshot, nil
						})
					if handlerErr == nil {
						txnProduceState.sequences = updatedSequences
					}
				}
				s.txnMu.Unlock()
			} else {
				offsets, handlerErr = s.txnLog.appendTopicBatchFormatted(records, encode)
			}
			if handlerErr == nil {
				for key, result := range results {
					if result.count != 0 && result.baseOffset < 0 {
						result.baseOffset = offsets[result.first]
						results[key] = result
					}
				}
			}
		} else {
			handlerErr = s.deliverEvent(ctx, &MessagesEvent{Messages: allMsgs})
		}
		if handlerErr != nil {
			s.logger.Errorf("[conn:%d] Batch processing error: %v", connID, handlerErr)
		}
	}

	// Do not acknowledge a successful Produce until the accepted batch
	// sequences are durably recorded. If the sink already accepted data but
	// checkpointing fails, mark the transaction unresolved (it cannot commit
	// or accept more data until an explicit abort / restart).
	if txnProduceState != nil && s.orderedFormatter == nil {
		s.txnMu.Lock()
		if handlerErr == nil && sequenceChanged {
			txnProduceState.sequences = proposedSequences
		}
		if handlerErr != nil {
			txnProduceState.needsResolution = true
		}
		if (sequenceChanged && handlerErr == nil) || handlerErr != nil {
			if err := s.persistTxnStateLocked(); err != nil {
				handlerErr = err
				txnProduceState.needsResolution = true
			}
		}
		s.txnMu.Unlock()
	} else if txnProduceState != nil && handlerErr != nil {
		s.txnMu.Lock()
		txnProduceState.needsResolution = true
		s.txnMu.Unlock()
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
			if s.orderedFormatter != nil {
				partResp.BaseOffset = result.baseOffset
			}
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

// validateTxnBatchProducer checks every v2 record-batch header without
// decoding its records. A stale producer must not write into the current
// producer's transaction merely because it knows the same transactional.id.
func validateTxnBatchProducer(records []byte, producerID int64, epoch int16) int16 {
	for len(records) > 0 {
		if len(records) < 61 {
			return kerr.CorruptMessage.Code
		}
		batchLength := int64(int32(binary.BigEndian.Uint32(records[8:12])))
		if batchLength < 49 || batchLength > int64(len(records)-12) || records[16] != 2 {
			return kerr.CorruptMessage.Code
		}
		if int64(binary.BigEndian.Uint64(records[43:51])) != producerID ||
			int16(binary.BigEndian.Uint16(records[51:53])) != epoch {
			return kerr.ProducerFenced.Code
		}
		records = records[12+int(batchLength):]
	}
	return 0
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

	// Persist the producer assignment/epoch before returning it to Kafka.
	// This prevents a process restart from silently reusing a producer ID.
	if txnID != "" && s.cfg.TransactionalWrite {
		s.txnMu.Lock()
		if s.txnStoreErr != nil {
			s.txnMu.Unlock()
			return s.sendResponse(conn, connID, correlationID, &kmsg.InitProducerIDResponse{
				Version: apiVersion, ErrorCode: kerr.UnknownServerError.Code, ProducerID: -1,
			})
		}
		existing, ok := s.txnStates[txnID]
		if ok {
			if existing.ending {
				s.txnMu.Unlock()
				return s.sendResponse(conn, connID, correlationID, &kmsg.InitProducerIDResponse{
					Version: apiVersion, ErrorCode: kerr.ConcurrentTransactions.Code, ProducerID: -1,
				})
			}
			if existing.active {
				if existing.pendingAction == "commit" {
					s.txnMu.Unlock()
					return s.sendResponse(conn, connID, correlationID, &kmsg.InitProducerIDResponse{
						Version: apiVersion, ErrorCode: kerr.ConcurrentTransactions.Code, ProducerID: -1,
					})
				}
				existing.ending = true
				s.txnMu.Unlock()
				existing.inFlight.Wait()
				s.txnMu.Lock()
				if existing.pendingAction == "" {
					if s.orderedFormatter != nil {
						if err := s.txnLog.AppendTopicTerminal(txnID, existing.instance, "abort"); err != nil {
							existing.ending = false
							s.txnStoreErr = fmt.Errorf("ordered WAL zombie abort: %w", err)
							s.txnMu.Unlock()
							return s.sendResponse(conn, connID, correlationID, &kmsg.InitProducerIDResponse{
								Version: apiVersion, ErrorCode: kerr.UnknownServerError.Code, ProducerID: -1,
							})
						}
					}
					existing.pendingAction = "abort"
					if err := s.persistTxnStateLocked(); err != nil {
						existing.ending = false
						s.txnMu.Unlock()
						s.logger.Errorf("[conn:%d] InitProducerId: durable abort decision failed: %v", connID, err)
						return s.sendResponse(conn, connID, correlationID, &kmsg.InitProducerIDResponse{
							Version: apiVersion, ErrorCode: kerr.UnknownServerError.Code, ProducerID: -1,
						})
					}
				}
				instance := existing.instance
				s.txnMu.Unlock()
				var err error
				if s.orderedFormatter == nil {
					err = s.deliverEvent(ctx, &TxnAbortEvent{TransactionalID: txnID, TransactionInstance: instance})
				}
				// The ordered WAL's durable abort decision is sufficient
				// for zombie fencing. File publication is asynchronous.
				s.txnMu.Lock()
				existing.ending = false
				if err != nil {
					existing.needsResolution = true
					_ = s.persistTxnStateLocked()
					s.txnMu.Unlock()
					s.logger.Errorf("[conn:%d] InitProducerId: unable to abort old producer %s: %v", connID, txnID, err)
					return s.sendResponse(conn, connID, correlationID, &kmsg.InitProducerIDResponse{
						Version: apiVersion, ErrorCode: kerr.UnknownServerError.Code, ProducerID: -1,
					})
				}
				existing.active = false
				existing.needsResolution = false
				existing.hasOutcome = false
			}
			// A restart may have restored an irreversible decision without a
			// corresponding materializer outcome. Never fence it away and
			// start a new transaction: require operator recovery instead.
			if existing.pendingAction != "" && existing.restartRequired {
				s.txnMu.Unlock()
				return s.sendResponse(conn, connID, correlationID, &kmsg.InitProducerIDResponse{
					Version: apiVersion, ErrorCode: kerr.ConcurrentTransactions.Code, ProducerID: -1,
				})
			}
			if existing.epoch == math.MaxInt16 {
				existing.producerID = s.producerIDCounter.Add(1)
				existing.epoch = 0
			} else {
				existing.epoch++
			}
			existing.active = false
			existing.needsResolution = false
			existing.pendingAction = ""
			existing.hasOutcome = false
			existing.restartRequired = false
			existing.sequences = make(map[string]txnPartitionSequence)
			existing.topics = nil
			if err := s.persistTxnStateLocked(); err != nil {
				s.txnMu.Unlock()
				s.logger.Errorf("[conn:%d] InitProducerId: persist epoch change: %v", connID, err)
				return s.sendResponse(conn, connID, correlationID, &kmsg.InitProducerIDResponse{
					Version: apiVersion, ErrorCode: kerr.UnknownServerError.Code, ProducerID: -1,
				})
			}
			producerID, epoch := existing.producerID, existing.epoch
			s.txnMu.Unlock()
			s.logger.Debugf("[conn:%d] InitProducerId: txnID=%s, producerID=%d, epoch=%d (existing)",
				connID, txnID, producerID, epoch)
			return s.sendResponse(conn, connID, correlationID, &kmsg.InitProducerIDResponse{
				Version: apiVersion, ProducerID: producerID, ProducerEpoch: epoch,
			})
		}
		if len(s.txnStates) >= maxTxnStates {
			s.txnMu.Unlock()
			s.logger.Warnf("[conn:%d] Rejecting InitProducerId: too many transactional IDs tracked (%d)", connID, maxTxnStates)
			return s.sendResponse(conn, connID, correlationID, &kmsg.InitProducerIDResponse{
				Version: apiVersion, ErrorCode: kerr.UnknownServerError.Code, ProducerID: -1,
			})
		}
		producerID := s.producerIDCounter.Add(1)
		s.txnStates[txnID] = &txnState{
			producerID: producerID, epoch: 0,
			sequences: make(map[string]txnPartitionSequence),
		}
		if err := s.persistTxnStateLocked(); err != nil {
			s.txnMu.Unlock()
			s.logger.Errorf("[conn:%d] InitProducerId: persist new producer %s: %v", connID, txnID, err)
			return s.sendResponse(conn, connID, correlationID, &kmsg.InitProducerIDResponse{
				Version: apiVersion, ErrorCode: kerr.UnknownServerError.Code, ProducerID: -1,
			})
		}
		s.txnMu.Unlock()
		return s.sendResponse(conn, connID, correlationID, &kmsg.InitProducerIDResponse{
			Version: apiVersion, ProducerID: producerID, ProducerEpoch: 0,
		})
	}

	// Non-transactional idempotent producers also draw from the same
	// monotonically increasing ID space. Persist the counter when durable
	// transaction state is enabled so IDs cannot be reused after a restart.
	s.txnMu.Lock()
	if s.txnStoreErr != nil {
		s.txnMu.Unlock()
		return s.sendResponse(conn, connID, correlationID, &kmsg.InitProducerIDResponse{
			Version: apiVersion, ErrorCode: kerr.UnknownServerError.Code, ProducerID: -1,
		})
	}
	producerID := s.producerIDCounter.Add(1)
	if err := s.persistTxnStateLocked(); err != nil {
		s.txnMu.Unlock()
		return s.sendResponse(conn, connID, correlationID, &kmsg.InitProducerIDResponse{
			Version: apiVersion, ErrorCode: kerr.UnknownServerError.Code, ProducerID: -1,
		})
	}
	s.txnMu.Unlock()

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

	hasSupportedPartition := false
	for _, topic := range req.Topics {
		topicResp := kmsg.AddPartitionsToTxnResponseTopic{
			Topic: topic.Topic,
		}
		for _, partition := range topic.Partitions {
			code := int16(0)
			if partition != onlyPartition || !s.isTopicAllowed(topic.Topic) {
				code = kerr.UnknownTopicOrPartition.Code
			} else {
				hasSupportedPartition = true
			}
			topicResp.Partitions = append(topicResp.Partitions, kmsg.AddPartitionsToTxnResponseTopicPartition{
				Partition: partition,
				ErrorCode: code,
			})
		}
		resp.Topics = append(resp.Topics, topicResp)
	}

	// Validate producerID/epoch against txnState and mark the transaction as active.
	s.txnMu.Lock()
	st, stOK := s.txnStates[req.TransactionalID]
	if !stOK || st.producerID != req.ProducerID || st.epoch != req.ProducerEpoch || st.restartRequired {
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
	if st.ending || st.needsResolution || s.txnStoreErr != nil {
		s.txnMu.Unlock()
		for ti := range resp.Topics {
			for pi := range resp.Topics[ti].Partitions {
				resp.Topics[ti].Partitions[pi].ErrorCode = kerr.InvalidTxnState.Code
			}
		}
		return s.sendResponse(conn, connID, correlationID, resp)
	}
	// An invalid-only request must not create an active transaction.
	if !hasSupportedPartition {
		s.txnMu.Unlock()
		return s.sendResponse(conn, connID, correlationID, resp)
	}
	if !st.active {
		instance, instanceErr := newTxnInstance(req.TransactionalID)
		if instanceErr != nil {
			s.txnMu.Unlock()
			for ti := range resp.Topics {
				for pi := range resp.Topics[ti].Partitions {
					resp.Topics[ti].Partitions[pi].ErrorCode = kerr.UnknownServerError.Code
				}
			}
			return s.sendResponse(conn, connID, correlationID, resp)
		}
		st.instance = instance
		st.topics = nil
		st.active = true
		st.hasOutcome = false
		st.pendingAction = ""
	}
	// All supported topic participants must be persisted, including topics
	// enrolled AFTER the transaction was already active. Retried enrollment
	// is idempotent, and rejected partitions never enter the participant set.
	participants := make(map[string]bool, len(st.topics))
	for _, topic := range st.topics { participants[topic] = true }
	changed := false
	for _, topic := range req.Topics {
		if !s.isTopicAllowed(topic.Topic) { continue }
		for _, partition := range topic.Partitions {
			if partition == onlyPartition && !participants[topic.Topic] {
				participants[topic.Topic] = true
				st.topics = append(st.topics, topic.Topic)
				changed = true
			}
		}
	}
	if changed { sort.Strings(st.topics) }
	if changed || len(st.topics)==0 {
		if err := s.persistTxnStateLocked(); err != nil {
			s.txnMu.Unlock()
			s.logger.Errorf("[conn:%d] AddPartitionsToTxn: persist active participant set: %v", connID, err)
			for ti := range resp.Topics {
				for pi := range resp.Topics[ti].Partitions {
					resp.Topics[ti].Partitions[pi].ErrorCode = kerr.UnknownServerError.Code
				}
			}
			return s.sendResponse(conn, connID, correlationID, resp)
		}
	}
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

	// Never transition to inactive before the output writer has succeeded.
	// A failed EndTxn leaves the transaction unresolved: a retry or abort is
	// allowed, but new Produce/AddPartitions requests are blocked.
	s.txnMu.Lock()
	st, ok := s.txnStates[req.TransactionalID]
	switch {
	case !ok || st.producerID != req.ProducerID || st.epoch != req.ProducerEpoch:
		s.txnMu.Unlock()
		resp.ErrorCode = kerr.ProducerFenced.Code
		return s.sendResponse(conn, connID, correlationID, resp)
	case st.ending:
		s.txnMu.Unlock()
		resp.ErrorCode = kerr.ConcurrentTransactions.Code
		return s.sendResponse(conn, connID, correlationID, resp)
	case s.txnStoreErr != nil:
		s.txnMu.Unlock()
		resp.ErrorCode = kerr.UnknownServerError.Code
		return s.sendResponse(conn, connID, correlationID, resp)
	case !st.active:
		// A lost EndTxn response can be retried without duplicating commits.
		repeated := st.hasOutcome && st.lastCommit == req.Commit
		s.txnMu.Unlock()
		if !repeated {
			resp.ErrorCode = kerr.InvalidTxnState.Code
		}
		return s.sendResponse(conn, connID, correlationID, resp)
	}
	if st.pendingAction != "" && st.pendingAction != action {
		s.txnMu.Unlock()
		resp.ErrorCode = kerr.InvalidTxnState.Code
		return s.sendResponse(conn, connID, correlationID, resp)
	}
	st.ending = true // fence new Produce before draining admitted requests
	s.txnMu.Unlock()
	st.inFlight.Wait()

	s.txnMu.Lock()
	// A failed Produce may have appended partial data to the WAL or staged
	// files. Never commit it, even if the producer sends EndTxn(commit).
	if req.Commit && st.needsResolution && st.pendingAction == "" {
		st.ending = false
		s.txnMu.Unlock()
		resp.ErrorCode = kerr.InvalidTxnState.Code
		return s.sendResponse(conn, connID, correlationID, resp)
	}
	if st.pendingAction == "" {
		// One atomic WAL frame contains the outcome and every enrolled
		// topic's control-marker offset. Commit success never depends on
		// when the per-topic file materializer finishes publishing.
		if s.orderedFormatter != nil {
			if err := s.txnLog.AppendTopicTerminal(req.TransactionalID, st.instance, action); err != nil {
				st.ending = false
				s.txnStoreErr = fmt.Errorf("ordered WAL terminal decision: %w", err)
				s.txnMu.Unlock()
				resp.ErrorCode = kerr.UnknownServerError.Code
				return s.sendResponse(conn, connID, correlationID, resp)
			}
		}
		st.pendingAction = action
		if err := s.persistTxnStateLocked(); err != nil {
			st.ending = false
			s.txnMu.Unlock()
			s.logger.Errorf("[conn:%d] EndTxn: failed to persist decision: %v", connID, err)
			resp.ErrorCode = kerr.UnknownServerError.Code
			return s.sendResponse(conn, connID, correlationID, resp)
		}
	}
	instance := st.instance
	s.txnMu.Unlock()

	var evt Event
	if req.Commit {
		evt = &TxnCommitEvent{TransactionalID: req.TransactionalID, TransactionInstance: instance}
	} else {
		evt = &TxnAbortEvent{TransactionalID: req.TransactionalID, TransactionInstance: instance}
	}
	var err error
	if s.orderedFormatter == nil {
		err = s.deliverEvent(ctx, evt)
	}
	// With ordered output the durable coordinator decision is sufficient for
	// EndTxn success. A separate materializer advances topic files later.

	s.txnMu.Lock()
	st.ending = false
	if err != nil {
		st.needsResolution = true
	} else {
		st.active = false
		st.needsResolution = false
		st.hasOutcome = true
		st.lastCommit = req.Commit
		st.pendingAction = ""
	}
	if persistErr := s.persistTxnStateLocked(); persistErr != nil {
		if err == nil {
			err = persistErr // external commit may have succeeded: never fake an ack
		}
		st.needsResolution = true
	}
	s.txnMu.Unlock()
	if err != nil {
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
