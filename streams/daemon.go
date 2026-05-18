package streams

import (
	"context"
	"log/slog"
	"time"
)

type StreamFunc func(context.Context) error

type Config struct {
	ReconnectDelay time.Duration
}

type Daemon struct {
	cfg Config
}

func New(cfg Config) *Daemon {
	if cfg.ReconnectDelay == 0 {
		cfg.ReconnectDelay = time.Second
	}
	return &Daemon{cfg: cfg}
}

func (d *Daemon) Run(ctx context.Context, f StreamFunc) error {
	for {
		if err := f(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Error("stream error, reconnecting", "err", err, "delay", d.cfg.ReconnectDelay)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d.cfg.ReconnectDelay):
		}
	}
}
