package driver

type PrepareBatchOptions struct {
	ReleaseConnection bool
	CloseQuery        bool
}

type PrepareBatchOption func(options *PrepareBatchOptions)

func WithReleaseConnection() PrepareBatchOption {
	return func(options *PrepareBatchOptions) {
		options.ReleaseConnection = true
	}
}

// WithCloseQuery fix not generate QueryFinish log in system.query_log when use batch.Flush
func WithCloseQuery() PrepareBatchOption {
	return func(options *PrepareBatchOptions) {
		options.CloseQuery = true
	}
}
