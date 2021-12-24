package aliyun_datahub_helper

import (
	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
)

// NewTupleRecord 创建 Tuple 类型的记录
func NewTupleRecord(schema *datahub.RecordSchema, data map[string]interface{}, attributes map[string]string) (record *datahub.TupleRecord) {
	record = datahub.NewTupleRecord(schema, 0)
	if data != nil {
		for key, val := range data {
			record.SetValueByName(key, val)
		}
	}
	setRecordAttributes(record, attributes)
	return
}

// NewBlobRecord 创建 Blob 类型的记录
func NewBlobRecord(data []byte, attributes map[string]string) (record *datahub.BlobRecord) {
	record = datahub.NewBlobRecord(data, 0)
	setRecordAttributes(record, attributes)
	return
}

// 设置记录的属性
func setRecordAttributes(record datahub.IRecord, attributes map[string]string) {
	if attributes != nil {
		for key, val := range attributes {
			record.SetAttribute(key, val)
		}
	}
}
