package aliyun_datahub_helper

import "github.com/aliyun/aliyun-datahub-sdk-go/datahub"

// SubscribeOptions 数据订阅选项
type SubscribeOptions struct {
	ProjectName       string                // Project，必填
	TopicName         string                // Topic，必填
	SubscribeId       string                // 订阅标识，必填
	Shards            []string              // 要订阅的 shardId 列表，可选；缺省时表示订阅所有 shard
	LimitPerGet       int                   // 每次获取数据条数限制，可选；默认为 100 条
	CommitOffsetCount int                   // 批量提交 offset 的数量，可选；默认为 1000 条
	CursorType        datahub.CursorType    // 消费游标类型，可选；缺省时第一次消息默认为 LATEST，非第一次消费为 SEQUENCE
	CursorParameter   int64                 // 消费游标获取参数，可选；当 CursorType 为 SEQUENCE 或 SYSTEM_TIME 时必填
	ConsumeRetryLimit int                   // 消费失败重试次数，可选；默认为 0 不重试
	RecordType        *datahub.RecordType   // 记录类型，可选；缺省时自动查询获取
	RecordSchema      *datahub.RecordSchema // 记录的 Schema，可选；缺省时自动查询获取
}

// StandardSubscribeOptions 规范数据订阅选项，设置缺省字段的默认值
func StandardSubscribeOptions(opts *SubscribeOptions) *SubscribeOptions {
	if opts.LimitPerGet <= 0 {
		opts.LimitPerGet = 100
	}
	if opts.CommitOffsetCount <= 0 {
		opts.CommitOffsetCount = 1000
	}
	if opts.ConsumeRetryLimit < 0 {
		opts.ConsumeRetryLimit = 0
	}

	return opts
}
