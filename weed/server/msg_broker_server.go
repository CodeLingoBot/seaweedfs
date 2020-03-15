package weed_server

import (
	"context"
	"fmt"
	"sort"
	"time"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type MessageBrokerOption struct {
	Filers             []string
	DefaultReplication string
	MaxMB              int
	Port               int
	Cipher             bool
}

type MessageBroker struct {
	option         *MessageBrokerOption
	grpcDialOption grpc.DialOption
	knownFilers    []string
	knownBrokers   []string
}

func NewMessageBroker(option *MessageBrokerOption) (messageBroker *MessageBroker, err error) {

	messageBroker = &MessageBroker{
		option:         option,
		grpcDialOption: security.LoadClientTLS(util.GetViper(), "grpc.msg_broker"),
	}

	go messageBroker.loopForEver()

	return messageBroker, nil
}

func (broker *MessageBroker) loopForEver() {

	for {
		broker.checkPeers()
		// do not need to check often to have a slow changing topology
		time.Sleep(30 * time.Second)
	}

}

func (broker *MessageBroker) checkPeers() {

	// contact a filer about masters
	var masters []string
	startingFilers := append(broker.knownFilers, broker.option.Filers...)
	for _, filer := range startingFilers {
		err := broker.withGrpcFilerClient(filer, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
			if err != nil {
				return err
			}
			masters = append(masters, resp.Masters...)
			return nil
		})
		if err != nil {
			fmt.Printf("failed to read masters from %+v: %v\n", broker.option.Filers, err)
			return
		}
	}
	masters = dedup(masters)

	// contact each masters for filers
	var filers []string
	for _, master := range masters {
		err := broker.withMasterClient(master, func(client master_pb.SeaweedClient) error {
			resp, err := client.ListMasterClients(context.Background(), &master_pb.ListMasterClientsRequest{
				ClientType: "filer",
			})
			if err != nil {
				return err
			}

			fmt.Printf("filers: %+v\n", resp.GrpcAddresses)
			filers = append(filers, resp.GrpcAddresses...)

			return nil
		})
		if err != nil {
			fmt.Printf("failed to list filers: %v\n", err)
			return
		}
	}
	filers = dedup(filers)
	sort.Strings(filers)
	broker.knownFilers = filers

	// contact each filer about brokers
	var peers []string
	for _, filer := range filers {
		err := broker.withGrpcFilerClient(filer, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.ListFilerClients(context.Background(), &filer_pb.ListFilerClientsRequest{
				ClientType: "msg.broker",
			})
			if err != nil {
				return err
			}
			peers = append(peers, resp.GrpcAddresses...)
			return nil
		})
		if err != nil {
			fmt.Printf("failed to read masters from %+v: %v\n", broker.option.Filers, err)
			return
		}
	}
	broker.knownBrokers = peers

}

func (broker *MessageBroker) withGrpcFilerClient(filer string, fn func(filer_pb.SeaweedFilerClient) error) error {

	return pb.WithGrpcFilerClient(filer, broker.grpcDialOption, fn)

}

func (broker *MessageBroker) withMasterClient(master string, fn func(client master_pb.SeaweedClient) error) error {

	return pb.WithMasterClient(master, broker.grpcDialOption, func(client master_pb.SeaweedClient) error {
		return fn(client)
	})

}

func dedup(list []string) (deduped []string) {
	m := make(map[string]bool)
	for _, t := range list {
		m[t] = true
	}
	for k := range m {
		deduped = append(deduped, k)
	}
	return
}
