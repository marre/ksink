package ksrv

import (
	"context"
	"crypto/sha256"
	"fmt"
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
		kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.Produce), MinVersion: 0, MaxVersion: 9},
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
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("failed to parse port %s: %w", portStr, err)
	}

	resp := &kmsg.MetadataResponse{
		Version: apiVersion,
		Brokers: []kmsg.MetadataResponseBroker{
			{
				NodeID: 1,
				Host:   host,
				Port:   int32(port),
			},
		},
		ControllerID: 1,
		ClusterID:    kmsg.StringPtr("ksrv-cluster"),
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

	for _, topicData := range req.Topics {
		topicResp := kmsg.ProduceResponseTopic{
			Topic: topicData.Topic,
		}

		for _, partData := range topicData.Partitions {
			partResp := kmsg.ProduceResponseTopicPartition{
				Partition: partData.Partition,
			}

			if !s.isTopicAllowed(topicData.Topic) {
				s.logger.Warnf("[conn:%d] Rejected produce to disallowed topic: %s", connID, topicData.Topic)
				partResp.ErrorCode = kerr.UnknownTopicOrPartition.Code
				topicResp.Partitions = append(topicResp.Partitions, partResp)
				continue
			}

			batch, err := s.parseRecords(connID, topicData.Topic, partData.Partition, partData.Records, remoteAddr)
			if err != nil {
				s.logger.Errorf("[conn:%d] Failed to parse records for %s/%d: %v", connID, topicData.Topic, partData.Partition, err)
				partResp.ErrorCode = kerr.CorruptMessage.Code
				topicResp.Partitions = append(topicResp.Partitions, partResp)
				continue
			}

			if len(batch) > 0 {
				err = s.handler(ctx, batch)
				if err != nil {
					s.logger.Errorf("[conn:%d] Handler error for %s/%d: %v", connID, topicData.Topic, partData.Partition, err)
					partResp.ErrorCode = kerr.UnknownServerError.Code
				}
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

func (s *Server) handleInitProducerId(conn net.Conn, connID uint64, correlationID int32, apiVersion int16, state *connState) error {
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
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("failed to parse port %s: %w", portStr, err)
	}

	resp := &kmsg.FindCoordinatorResponse{
		Version:        apiVersion,
		NodeID:         1,
		Host:           host,
		Port:           int32(port),
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
				Port:   int32(port),
			})
		}
	}

	s.logger.Debugf("[conn:%d] FindCoordinator response: host=%s, port=%d", connID, host, port)
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
