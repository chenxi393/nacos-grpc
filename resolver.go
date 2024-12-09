package resolver

import (
	"context"
	"sync"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	gresolver "google.golang.org/grpc/resolver"
)

type resolver struct {
	c      naming_client.INamingClient
	target string
	cc     gresolver.ClientConn
	wch    WatchChannel
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (r *resolver) watch() {
	defer r.wg.Done()

	allUps := make(map[string]*Update)
	for {
		select {
		case <-r.ctx.Done():
			return
		case ups, ok := <-r.wch:
			if !ok {
				return
			}

			for _, up := range ups {
				if up.Endpoint.Addr == "" {
					delete(allUps, up.Key)
				} else {
					allUps[up.Key] = up
				}
			}

			addrs := convertToGRPCAddress(allUps)
			r.cc.UpdateState(gresolver.State{Addresses: addrs})
		}
	}
}

func convertToGRPCAddress(ups map[string]*Update) []gresolver.Address {
	var addrs []gresolver.Address
	for _, up := range ups {
		addr := gresolver.Address{
			Addr:     up.Endpoint.Addr,
			Metadata: up.Endpoint.Metadata,
		}
		addrs = append(addrs, addr)
	}
	return addrs
}

// ResolveNow is a no-op here.
// It's just a hint, resolver can ignore this if it's not necessary.
func (r *resolver) ResolveNow(gresolver.ResolveNowOptions) {}

func (r *resolver) Close() {
	r.cancel()
	r.wg.Wait()
}
