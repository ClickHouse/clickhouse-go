package clickhouse

import (
	"log/slog"
	"os"
)

type logbase struct {
	logger *slog.Logger
	common []any
}

func initLogger(logLevel slog.Level, common []any) *logbase {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	return &logbase{
		logger: logger,
		common: common,
	}
}
