package wire

import (
	"github.com/twmb/franz-go/pkg/kmsg"
	"testing"
)

func TestProduceV9KeepsUnknownLeaderWithoutNewerVersionTag(t *testing.T) {
	d := testDispatcher(t)
	req := kmsg.NewPtrProduceRequest()
	req.Acks = 1
	req.Topics = []kmsg.ProduceRequestTopic{{Topic: "response-test", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 0}}}}
	resp, _ := exchange(t, d, int16(kmsg.Produce), 9, req)
	part := resp.(*kmsg.ProduceResponse).Topics[0].Partitions[0]
	if part.ErrorCode != errCodeCorruptMessage {
		t.Fatalf("empty records error = %d", part.ErrorCode)
	}
	if part.CurrentLeader.LeaderID != -1 || part.CurrentLeader.LeaderEpoch != -1 {
		t.Fatalf("v9 response contains the unsupported CurrentLeader tag: %+v", part.CurrentLeader)
	}
	if part.LogAppendTime != -1 || part.LogStartOffset != -1 {
		t.Fatalf("unknown timestamps/offsets must stay -1: %+v", part)
	}
}

func TestCreateTopicsReturnsAnEmptyConfigurationList(t *testing.T) {
	for _, version := range []int16{5, 6} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			req := kmsg.NewPtrCreateTopicsRequest()
			req.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: "admin-response-test", NumPartitions: 1, ReplicationFactor: 1}}
			resp, _ := exchange(t, testDispatcher(t), int16(kmsg.CreateTopics), version, req)
			topic := resp.(*kmsg.CreateTopicsResponse).Topics[0]
			if topic.ErrorCode != 0 || topic.NumPartitions != 1 {
				t.Fatalf("create failed: %+v", topic)
			}
			if topic.Configs == nil {
				t.Fatal("successful Java Admin response must have an empty list, not null configs")
			}
		})
	}
}
