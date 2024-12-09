package resolver

import (
	"context"
	"errors"
	"strconv"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

type WatchChannel <-chan []*Update

type Update struct {
	Key      string
	Endpoint Endpoint
}

type Endpoint struct {
	// Addr is the server address on which a connection will be established.
	Addr string

	// Metadata is the information associated with Addr, which may be used
	// to make load balancing decision.
	Metadata interface{}
}

type endpointManager struct {
	// Client is an initialized nacos naming client.
	client naming_client.INamingClient
	target string
	group  string
}

func NewManager(client naming_client.INamingClient, target string, group string) (*endpointManager, error) {
	if client == nil {
		return nil, errors.New("invalid client")
	}

	if target == "" {
		return nil, errors.New("invalid target")
	}

	em := &endpointManager{
		client: client,
		target: target,
		group:  group,
	}
	return em, nil
}

func (m *endpointManager) NewWatchChannel(ctx context.Context) (WatchChannel, error) {
	instances, err := m.client.SelectInstances(vo.SelectInstancesParam{
		ServiceName: m.target,
		GroupName:   m.group,
		HealthyOnly: true,
	})
	if err != nil {
		return nil, err
	}
	if len(instances) <= 0 {
		return nil, errors.New("no instance found")
	}

	initUpdates := make([]*Update, 0, len(instances))
	for _, kv := range instances {
		up := &Update{
			Key:      string(kv.ServiceName),
			Endpoint: Endpoint{Addr: kv.Ip + ":" + strconv.FormatUint(kv.Port, 10), Metadata: kv.Metadata},
		}
		initUpdates = append(initUpdates, up)
	}

	upch := make(chan []*Update, 1)
	if len(initUpdates) > 0 {
		upch <- initUpdates
	}
	go m.watch(ctx, upch)
	return upch, nil
}

// 监听变更去做删减
func (m *endpointManager) watch(ctx context.Context, upch chan []*Update) {
	defer close(upch)
	// 订阅变更
	err := m.client.Subscribe(&vo.SubscribeParam{
		ServiceName: m.target,
		GroupName:   m.group,
		SubscribeCallback: func(services []model.Instance, err error) {
			if err != nil {
				panic(err)
			}
			for _, kv := range services {
				up := &Update{
					Key:      string(kv.ServiceName),
					Endpoint: Endpoint{Addr: kv.Ip, Metadata: kv.Metadata},
				}
				upch <- []*Update{up}
			}
		},
	})
	if err != nil {
		panic(err)
	}
	<-ctx.Done()
}
