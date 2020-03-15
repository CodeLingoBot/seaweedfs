package weed_server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/queue_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
)

func (broker *MessageBroker) ConfigureTopic(context.Context, *queue_pb.ConfigureTopicRequest) (*queue_pb.ConfigureTopicResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (broker *MessageBroker) DeleteTopic(context.Context, *queue_pb.DeleteTopicRequest) (*queue_pb.DeleteTopicResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (broker *MessageBroker) StreamWrite(stream queue_pb.SeaweedQueue_StreamWriteServer) error {
	isRunning := true
	defer func() {
		isRunning = false
	}()

	var bd = &BrokerData{}

	var wg sync.WaitGroup

	go func() {
		defer wg.Done()
		for isRunning {
			time.Sleep(3 * time.Second)

			bd.MaybeSwapData()
			outgoingData := bd.outgoingData
			if len(outgoingData) == 0 {
				continue
			}
			data := packageData(outgoingData)

			filderGrpcAddress := broker.knownFilers[0]
			var assignResult *filer_pb.AssignVolumeResponse
			var assignError error
			assignRequest := &filer_pb.AssignVolumeRequest{
				Count:       1,
				Collection:  "",
				Replication: "",
				TtlSec:      0,
				DataCenter:  "",
				ParentPath:  "",
			}
			err := broker.withGrpcFilerClient(filderGrpcAddress, func(filerClient filer_pb.SeaweedFilerClient) error {
				assignResult, assignError = filerClient.AssignVolume(context.Background(), assignRequest)
				if assignError != nil {
					return fmt.Errorf("assign volume failure %v: %v", assignRequest, assignError)
				}
				if assignResult.Error != "" {
					return fmt.Errorf("assign volume failure %v: %v", assignRequest, assignResult.Error)
				}
				return nil
			})

			if err != nil {
				// return the outgoingData back???
				continue
			}

			targetUrl := "http://" + assignResult.Url + "/" + assignResult.FileId
			uploadResult, err := operation.UploadData(targetUrl, "", broker.option.Cipher, data, false, "", nil, security.EncodedJwt(assignResult.Auth))
			if err != nil {
				glog.Errorf("upload data %v to %s: %v\n", fileName, targetUrl, err)
			} else if uploadResult.Error != "" {
				glog.Errorf("upload %v to %s result: %v\n", fileName, targetUrl, uploadResult.Error)
			}

		}
	}()

	for isRunning {
		req, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("receive message write: %v", err)
		}
		bd.AddIncomingData(req.Data)
		resp := &queue_pb.WriteMessageResponse{
			AckNs: req.EventNs,
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}

	return nil
}

func packageData(outgoingData [][]byte) []byte {
	return nil
}

func (broker *MessageBroker) StreamRead(*queue_pb.ReadMessageRequest, queue_pb.SeaweedQueue_StreamReadServer) error {
	return fmt.Errorf("not implemented")
}

type BrokerData struct {
	incomingData [][]byte
	outgoingData [][]byte
	sync.Mutex
}

func (b *BrokerData) AddIncomingData(data []byte) {
	b.Lock()
	defer b.Unlock()

	b.incomingData = append(b.incomingData, data)
}

func (b *BrokerData) MaybeSwapData() {
	b.Lock()
	defer b.Unlock()

	if len(b.outgoingData) > 0 {
		return
	}

	b.incomingData, b.outgoingData = b.outgoingData, b.incomingData
}

func (b *BrokerData) MarkOutgoingDataSent() {
	b.Lock()
	defer b.Unlock()

	b.outgoingData = b.outgoingData[:0]
}
