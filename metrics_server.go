package main

import (
	"context"
	"expvar"
	"net/http"
	"scanner_eth/config"
	"time"

	"github.com/sirupsen/logrus"
)

func startMetricsServer(conf config.Metrics) *http.Server {
	if !conf.Enable {
		return nil
	}

	addr := conf.Addr
	if addr == "" {
		addr = "127.0.0.1:6060"
	}
	path := conf.Path
	if path == "" {
		path = "/debug/vars"
	}

	mux := http.NewServeMux()
	mux.Handle(path, expvar.Handler())

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logrus.Infof("metrics server listening addr:%v path:%v", addr, path)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logrus.Warnf("metrics server stopped with error: %v", err)
		}
	}()

	return srv
}

func shutdownMetricsServer(ctx context.Context, srv *http.Server) {
	if srv == nil {
		return
	}
	if err := srv.Shutdown(ctx); err != nil {
		logrus.Warnf("metrics server shutdown failed: %v", err)
	}
}
