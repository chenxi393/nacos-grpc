package resolver

import (
	"context"
	"strings"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"google.golang.org/grpc/codes"
	gresolver "google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
)

type builder struct {
	c     naming_client.INamingClient
	group string
}

func (b builder) Build(target gresolver.Target, cc gresolver.ClientConn, opts gresolver.BuildOptions) (gresolver.Resolver, error) {
	endpoint := target.URL.Path
	if endpoint == "" {
		endpoint = target.URL.Opaque
	}
	endpoint = strings.Trim(endpoint, "/")
	r := &resolver{
		c:      b.c,
		target: endpoint,
		cc:     cc,
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())

	em, err := NewManager(r.c, r.target, b.group)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "resolver: failed to new endpoint manager: %s", err)
	}
	r.wch, err = em.NewWatchChannel(r.ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "resolver: failed to new watch channer: %s", err)
	}

	r.wg.Add(1)
	go r.watch()
	return r, nil
}

func (b builder) Scheme() string {
	return "nacos"
}

// NewBuilder creates a resolver builder.
func NewBuilder(client naming_client.INamingClient, group string) (gresolver.Builder, error) {
	return builder{c: client, group: group}, nil
}
