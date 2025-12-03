package clickhouse

import (
	"log/slog"
	"os"
)

type logCore struct {
	logger *slog.Logger
	common []any
}

func initLogger(logLevel slog.Level, common []any) *logCore {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	return &logCore{
		logger: logger,
		common: common,
	}
}
