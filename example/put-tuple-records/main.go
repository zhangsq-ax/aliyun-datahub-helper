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
	TupleTopicName  = "tuple-topic"
)

func main() {
	// create helper instance
	helper := adh.NewDataHubHelper(&adh.DataHubHelperOptions{
		Endpoint:        Endpoint,
		AccessKeyId:     AccessKeyId,
		AccessKeySecret: AccessKeySecret,
	})

	/*
		TupleTopic Schema
		{"fields":[{"name":"key","type":"STRING","comment":"标识"},{"name":"value","type":"INTEGER","comment":"值"}]}
	*/
	result, err := helper.PutTupleRecords(&adh.PutRecordsOptions{
		ProjectName: ProjectName,
		TopicName:   TupleTopicName,
	}, []map[string]interface{}{
		{
			"key":   "foo",
			"value": 1,
		},
		{
			"key":   "bar",
			"value": 2,
		},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(result)
}
