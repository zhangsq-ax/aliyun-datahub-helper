package aliyun_datahub_helper

// PutRecordsOptions 推送数据选项
type PutRecordsOptions struct {
	ProjectName string
	TopicName   string
	//ShardId     string // 可选，如不设置则系统决定写入哪个 shard
}
