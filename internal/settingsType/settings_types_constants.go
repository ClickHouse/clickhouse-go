package settingsType

const (
	// MaxExecutionTime is the query timeout in seconds (int).
	MaxExecutionTime string = "max_execution_time"

	// MaxMemoryUsage is the memory limit per query (int).
	MaxMemoryUsage string = "max_memory_usage"

	// MaxBlockSize is the block size for processing (int).
	MaxBlockSize string = "max_block_size"

	// Readonly sets read-only mode: 1 = read-only, 2 = read-only + settings changes (int).
	Readonly string = "readonly"
)
