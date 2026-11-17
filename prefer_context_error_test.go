package clickhouse

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"
)

func TestPreferContextError(t *testing.T) {
	t.Parallel()

	deadlineErr := &net.OpError{Op: "read", Err: os.ErrDeadlineExceeded}

	t.Run("nil err stays nil", func(t *testing.T) {
		if got := preferContextError(context.Background(), nil); got != nil {
			t.Fatalf("got %v, want nil", got)
		}
	})

	t.Run("live context keeps socket error", func(t *testing.T) {
		got := preferContextError(context.Background(), deadlineErr)
		if !errors.Is(got, os.ErrDeadlineExceeded) {
			t.Fatalf("got %v, want os.ErrDeadlineExceeded", got)
		}
	})

	t.Run("expired context returns DeadlineExceeded", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		got := preferContextError(ctx, deadlineErr)
		if !errors.Is(got, context.DeadlineExceeded) {
			t.Fatalf("got %v, want context.DeadlineExceeded", got)
		}
	})

	t.Run("canceled context returns Canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		got := preferContextError(ctx, deadlineErr)
		if !errors.Is(got, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", got)
		}
	})
}
