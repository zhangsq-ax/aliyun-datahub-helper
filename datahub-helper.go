package aliyun_datahub_helper

import (
	"context"
	"fmt"
	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
	"time"
)

// DataHubHelper 阿里云数据总线服务操作助手
type DataHubHelper struct {
	dh datahub.DataHubApi
}

// NewDataHubHelper 创建新的数据总线服务操作助手实例
func NewDataHubHelper(opts *DataHubHelperOptions) *DataHubHelper {
	dh := datahub.New(opts.AccessKeyId, opts.AccessKeySecret, opts.Endpoint)
	return &DataHubHelper{
		dh: dh,
	}
}

// PutRecords 推送数据
func (h *DataHubHelper) PutRecords(opts *PutRecordsOptions, records []datahub.IRecord) (*datahub.PutRecordsResult, error) {
	return h.dh.PutRecords(opts.ProjectName, opts.TopicName, records)
	/*if opts.ShardId == "" {
		result, err := h.dh.PutRecords(opts.ProjectName, opts.TopicName, records)
		fmt.Println(result, err)
	} else {
		result, err := h.dh.PutRecordsByShard(opts.ProjectName, opts.TopicName, opts.ShardId, records)
		fmt.Println(result, err)
	}*/
}

// GetTopic 获取 Topic 信息
func (h *DataHubHelper) GetTopic(projectName, topicName string) (*datahub.GetTopicResult, error) {
	return h.dh.GetTopic(projectName, topicName)
}

// GetRecordSchema 获取指定 Topic 的 Schema，如果是 Blog 类型的 Topic 则返回 nil
func (h *DataHubHelper) GetRecordSchema(projectName, topicName string) (schema *datahub.RecordSchema, err error) {
	topic, err := h.GetTopic(projectName, topicName)
	if err != nil {
		return
	}
	schema = topic.RecordSchema
	return
}

// NewTupleRecords 创建 Tuple 类型的记录数组
func (h *DataHubHelper) NewTupleRecords(projectName, topicName string, data []map[string]interface{}) (records []datahub.IRecord, err error) {
	schema, err := h.GetRecordSchema(projectName, topicName)
	if err != nil {
		return
	}
	if schema != nil {
		for _, item := range data {
			records = append(records, NewTupleRecord(schema, item, nil))
		}
	}

	return
}

// Subscribe 订阅数据
func (h *DataHubHelper) Subscribe(ctx context.Context, opts *SubscribeOptions) (recordChan chan datahub.IRecord, errChan chan error, err error) {
	// 规范设置
	opts = StandardSubscribeOptions(opts)

	// 如果未设置 RecordType
	if opts.RecordType == nil {
		// 获取 topic 信息
		var topic *datahub.GetTopicResult
		topic, err = h.dh.GetTopic(opts.ProjectName, opts.TopicName)
		if err != nil {
			return
		}
		opts.RecordType = &topic.RecordType
		if topic.RecordType == datahub.TUPLE {
			opts.RecordSchema = topic.RecordSchema
		}
	}

	// 如果未设置 shards 需要获取 shard 列表并消费所有 shard
	shards := opts.Shards
	if shards == nil || len(shards) == 0 {
		shards, err = h.getTopicShards(opts)
		if err != nil {
			return
		}
	}

	recordChan = make(chan datahub.IRecord, 100)
	errChan = make(chan error, 100)

	for _, shardId := range shards {
		go h.consumeByShard(ctx, recordChan, errChan, opts, shardId)
	}

	return
}

// 消费指定 shard 的数据
func (h *DataHubHelper) consumeByShard(ctx context.Context, recordChannel chan datahub.IRecord, errorChannel chan error, opts *SubscribeOptions, shardId string) {
	// 获取 shard 消费游标
	cursor, err := h.getShardCursor(opts, shardId)
	if err != nil {
		errorChannel <- fmt.Errorf("failed to get cursor: %s - %v", shardId, err)
		return
	}
	recordCount := 0
	getRecordsFailedCount := 0
	var lastRecord datahub.IRecord
	for {
		select {
		case <-ctx.Done():
			if lastRecord != nil {
				err := h.commitShardOffset(opts, shardId, lastRecord)
				if err != nil {
					errorChannel <- fmt.Errorf("failed to commit offset: %s - %v", shardId, err)
				}
			}
			return
		default:
		}

		result, err := h.getRecords(opts, shardId, cursor)
		if err != nil {
			errorChannel <- fmt.Errorf("failed to get records: %s - %v", shardId, err)
			if getRecordsFailedCount < opts.ConsumeRetryLimit {
				getRecordsFailedCount += 1
				time.Sleep(time.Second * 3)
				continue
			} else {
				break
			}
		}
		if result.RecordCount == 0 {
			// 等待之前先提交一次 offset
			if lastRecord != nil {
				err := h.commitShardOffset(opts, shardId, lastRecord)
				if err != nil {
					errorChannel <- fmt.Errorf("failed to commit offset: %s - %v", shardId, err)
				} else {
					lastRecord = nil
				}
			}
			time.Sleep(time.Second * 5)
			continue
		}

		for _, record := range result.Records {
			recordChannel <- record
			lastRecord = record
			recordCount += 1

			// 如果满足设置的条数则提交 offset
			if recordCount >= opts.CommitOffsetCount {
				err := h.commitShardOffset(opts, shardId, lastRecord)
				if err != nil {
					errorChannel <- fmt.Errorf("failed to commit offset: %s - %v", shardId, err)
					continue
				}
				recordCount = 0
			}
		}

		// 设置下一轮消费的游标
		cursor = result.NextCursor
	}
}

// 提交 shard 消费的 offset
func (h *DataHubHelper) commitShardOffset(opts *SubscribeOptions, shardId string, lastRecord datahub.IRecord) error {
	offset, err := h.getShardOffset(opts, shardId)
	if err != nil {
		return err
	}
	offset.Sequence = lastRecord.GetSequence()
	offset.Timestamp = lastRecord.GetSystemTime()

	offsetMap := map[string]datahub.SubscriptionOffset{
		shardId: offset,
	}
	_, err = h.dh.CommitSubscriptionOffset(opts.ProjectName, opts.TopicName, opts.SubscribeId, offsetMap)
	return err
}

// 从指定 shard 中取得数据
func (h *DataHubHelper) getRecords(opts *SubscribeOptions, shardId, cursor string) (result *datahub.GetRecordsResult, err error) {
	if *opts.RecordType == datahub.BLOB {
		result, err = h.dh.GetBlobRecords(opts.ProjectName, opts.TopicName, shardId, cursor, opts.LimitPerGet)
	} else if *opts.RecordType == datahub.TUPLE {
		result, err = h.dh.GetTupleRecords(opts.ProjectName, opts.TopicName, shardId, cursor, opts.LimitPerGet, opts.RecordSchema)
	}
	return
}

// 获取指定 shard 的数据消费游标
func (h *DataHubHelper) getShardCursor(opts *SubscribeOptions, shardId string) (cursor string, err error) {
	if opts.CursorType == "" {
		// 没有设置消费游标类型则获取 shard 消费 offset
		var offset datahub.SubscriptionOffset
		offset, err = h.getShardOffset(opts, shardId)
		if err != nil {
			return
		}
		// 根据 shard 消费 offset 设置消费游标类型
		if offset.Sequence < 0 {
			// 从最新的开始消费
			opts.CursorType = datahub.LATEST
		} else {
			// 继续消费
			opts.CursorType = datahub.SEQUENCE
			opts.CursorParameter = offset.Sequence + 1
		}
	}

	var cursorResult *datahub.GetCursorResult
	if opts.CursorType == datahub.SEQUENCE || opts.CursorType == datahub.SYSTEM_TIME {
		cursorResult, err = h.dh.GetCursor(opts.ProjectName, opts.TopicName, shardId, opts.CursorType, opts.CursorParameter)
	} else {
		cursorResult, err = h.dh.GetCursor(opts.ProjectName, opts.TopicName, shardId, opts.CursorType)
	}
	if err != nil {
		return
	}
	cursor = cursorResult.Cursor

	return
}

// 获取指定 shard 的数据消费 offset
func (h *DataHubHelper) getShardOffset(opts *SubscribeOptions, shardId string) (offset datahub.SubscriptionOffset, err error) {
	session, err := h.dh.OpenSubscriptionSession(opts.ProjectName, opts.TopicName, opts.SubscribeId, []string{shardId})
	if err != nil {
		return
	}
	offset = session.Offsets[shardId]
	return
}

// 获取指定 Topic 的活动 shard
func (h *DataHubHelper) getTopicShards(opts *SubscribeOptions) (shardIds []string, err error) {
	result, err := h.dh.ListShard(opts.ProjectName, opts.TopicName)
	if err != nil {
		return
	}

	for _, shard := range result.Shards {
		// 只输出活动状态的 shard
		if shard.State == datahub.ACTIVE {
			shardIds = append(shardIds, shard.ShardId)
		}
	}
	return
}
