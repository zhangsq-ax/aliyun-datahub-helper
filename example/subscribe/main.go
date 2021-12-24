package main

import (
	"context"
	"fmt"
	adh "github.com/zhangsq-ax/aliyun-datahub-helper"
	"time"
)

const (
	Endpoint        = "https://xxx.xxx"
	AccessKeyId     = "XXXXXXXXXXXX"
	AccessKeySecret = "XXXXXXXXXXXXXXXXXXX"
	ProjectName     = "test-project"
	TopicName       = "tuple-topic"
	SubscribeId     = "XXXXXXXXXXXXXX"
)

func main() {
	// create helper instance
	helper := adh.NewDataHubHelper(&adh.DataHubHelperOptions{
		Endpoint:        Endpoint,
		AccessKeyId:     AccessKeyId,
		AccessKeySecret: AccessKeySecret,
	})

	// 20s 后停止，应该根据实际情况而定
	ctx, _ := context.WithTimeout(context.Background(), 20*time.Second)

	fmt.Println("Start consume ...")
	recordChan, errChan, err := helper.Subscribe(ctx, &adh.SubscribeOptions{
		ProjectName: ProjectName,
		TopicName:   TopicName,
		SubscribeId: SubscribeId,
	})
	if err != nil {
		panic(err)
	}

	for {
		select {
		case record := <-recordChan:
			fmt.Println(record)
		case err := <-errChan:
			fmt.Println(err)
		case <-ctx.Done():
			fmt.Println("Stop consume ...")
			return
		}
	}

}
