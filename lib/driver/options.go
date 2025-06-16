package driver

type PrepareBatchOptions struct {
	ReleaseConnection bool
	CloseOnFlush      bool
	HTTPSendOnFlush   bool
}

type PrepareBatchOption func(options *PrepareBatchOptions)

func WithReleaseConnection() PrepareBatchOption {
	return func(options *PrepareBatchOptions) {
		options.ReleaseConnection = true
	}
}

// WithCloseOnFlush closes batch INSERT query when Flush is executed
func WithCloseOnFlush() PrepareBatchOption {
	return func(options *PrepareBatchOptions) {
		options.CloseOnFlush = true
	}
}

// WithHTTPSendOnFlush sends a batch of data when Flush is called, only for HTTP connections.
func WithHTTPSendOnFlush() PrepareBatchOption {
	return func(options *PrepareBatchOptions) {
		options.HTTPSendOnFlush = true
	}
}
