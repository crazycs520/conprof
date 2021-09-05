// Copyright 2020 The conprof Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"time"

	"github.com/conprof/db/tsdb"
	"github.com/conprof/db/tsdb/wal"
	"github.com/go-kit/kit/log"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/tags"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/prober"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"gopkg.in/alecthomas/kingpin.v2"
)

type componentString string

func (c componentString) String() string {
	return string(c)
}

// registerStorage registers a sampler command.
func registerStorage(m map[string]setupFunc, app *kingpin.Application, name string, reloadCh chan struct{}) {
	cmd := app.Command(name, "Run a sampler, that appends profiles to a configured storage.")

	storagePath := cmd.Flag("storage.tsdb.path", "Directory to read storage from.").
		Default("./data").String()
	retention := extkingpin.ModelDuration(cmd.Flag("storage.tsdb.retention.time", "How long to retain raw samples on local storage. 0d - disables this retention").Default("15d"))
	grpcBindAddr, grpcGracePeriod, grpcCert, grpcKey, grpcClientCA := extkingpin.RegisterGRPCFlags(cmd)
	reqLogConfig := extkingpin.RegisterRequestLoggingFlags(cmd)

	m[name] = func(comp component.Component, g *run.Group, mux httpMux, probe prober.Probe, logger log.Logger, reg *prometheus.Registry, debugLogging bool) (prober.Probe, error) {
		tagOpts, grpcLogOpts, err := logging.ParsegRPCOptions("", reqLogConfig)
		if err != nil {
			return probe, errors.Wrap(err, "error while parsing config for request logging")
		}

		db, err := tsdb.Open(
			*storagePath,
			logger,
			prometheus.DefaultRegisterer,
			&tsdb.Options{
				RetentionDuration:      time.Duration(*retention).Milliseconds(),
				WALSegmentSize:         wal.DefaultSegmentSize,
				MinBlockDuration:       tsdb.DefaultBlockDuration,
				MaxBlockDuration:       time.Duration(*retention).Milliseconds() / 10,
				NoLockfile:             true,
				AllowOverlappingBlocks: false,
				WALCompression:         true,
				StripeSize:             tsdb.DefaultStripeSize,
			},
		)
		if err != nil {
			return probe, err
		}
		return runStorage(
			comp,
			g,
			probe,
			reg,
			logger,
			grpcLogOpts,
			tagOpts,
			db,
			*grpcBindAddr,
			time.Duration(*grpcGracePeriod),
			*grpcCert,
			*grpcKey,
			*grpcClientCA,
		)
	}
}

func runStorage(
	comp component.Component,
	g *run.Group,
	probe prober.Probe,
	reg *prometheus.Registry,
	logger log.Logger,
	grpcLogOpts []grpc_logging.Option,
	tagOpts []tags.Option,
	db *tsdb.DB,
	grpcBindAddr string,
	grpcGracePeriod time.Duration,
	grpcCert string,
	grpcKey string,
	grpcClientCA string,
) (prober.Probe, error) {
	grpcProbe := prober.NewGRPC()
	statusProber := prober.Combine(
		probe,
		grpcProbe,
		prober.NewInstrumentation(comp, logger, extprom.WrapRegistererWithPrefix("conprof_", reg)),
	)
	//maxBytesPerFrame := 1024 * 1024 * 2 // 2 Mb default, might need to be tuned later on.
	//s := store.NewProfileStore(logger, db, maxBytesPerFrame)

	srv := grpcserver.New(logger, reg, &opentracing.NoopTracer{}, grpcLogOpts, tagOpts, comp, grpcProbe,
		//grpcserver.WithServer(store.RegisterReadableStoreServer(s)),
		//grpcserver.WithServer(store.RegisterWritableStoreServer(s)),
		grpcserver.WithListen(grpcBindAddr),
		grpcserver.WithGracePeriod(grpcGracePeriod),
		grpcserver.WithGRPCServerOption(
			grpc.ChainUnaryInterceptor(
				otelgrpc.UnaryServerInterceptor(),
			),
		),
		grpcserver.WithGRPCServerOption(
			grpc.ChainStreamInterceptor(
				otelgrpc.StreamServerInterceptor(),
			),
		),
	)

	g.Add(func() error {
		statusProber.Ready()
		return srv.ListenAndServe()
	}, func(err error) {
		grpcProbe.NotReady(err)
		srv.Shutdown(err)
	})

	return statusProber, nil
}
