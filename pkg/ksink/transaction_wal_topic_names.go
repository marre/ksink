package ksink

import (
	"sort"
)

// TopicNames returns all logical topic streams known to this WAL. The topic
// registry is reconstructed from data and control markers on replay, and
// remains independent of the physical WAL segment layout.
func (l *TransactionLog) TopicNames() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	names := make([]string, 0, len(l.topicNext))
	for topic := range l.topicNext {
		names = append(names, topic)
	}
	sort.Strings(names)
	return names
}
