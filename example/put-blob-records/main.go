package main

import (
	"fmt"
	adh "github.com/zhangsq-ax/aliyun-datahub-helper"
)

const (
	Endpoint        = "https://xxx.xxx"
	AccessKeyId     = "XXXXXXXXXXXX"
	AccessKeySecret = "XXXXXXXXXXXXXXXXXXX"
	ProjectName     = "test-project"
	BlobTopicName   = "blob-topic"
)

func main() {
	// create helper instance
	helper := adh.NewDataHubHelper(&adh.DataHubHelperOptions{
		Endpoint:        Endpoint,
		AccessKeyId:     AccessKeyId,
		AccessKeySecret: AccessKeySecret,
	})

	result, err := helper.PutBlobRecords(&adh.PutRecordsOptions{
		ProjectName: ProjectName,
		TopicName:   BlobTopicName,
	}, [][]byte{
		[]byte("foo"),
		[]byte("bar"),
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(result)
}
