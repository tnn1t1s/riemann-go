// Command riemannd runs the engine behind the HTTP transport. It is the
// only place a listen address appears.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/tnn1t1s/riemann-go/engine"
	"github.com/tnn1t1s/riemann-go/stream"
	transport "github.com/tnn1t1s/riemann-go/transport/http"
)

// DefaultListen is the listen address. Loopback only until a token exists.
const DefaultListen = "127.0.0.1:5557"

// shutdownGrace is how long in-flight requests get after a signal.
// Default 2 s: longer than the admission deadline, short enough that a
// restart is not noticeable.
const shutdownGrace = 2 * time.Second

func main() {
	listen := flag.String("listen", DefaultListen, "listen address")
	shards := flag.Int("shards", engine.DefaultShards(), "host shards (engine.shards)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	eng := engine.New(engine.Config{Shards: *shards}, stream.StdClock{})
	eng.Start()
	defer eng.Stop()

	srv := &http.Server{Addr: *listen, Handler: transport.New(eng).Handler()}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()
	log.Info("listening", "addr", *listen, "shards", *shards)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
	log.Info("stopped")
}
