package clickhouse

const (
	ClientHelloPacket  = 0
	ClientQueryPacket  = 1
	ClientDataPacket   = 2
	ClientCancelPacket = 3
	ClientPingPacket   = 4
)

const (
	StateComplete = 2
)

const (
	ServerHelloPacket       = 0
	ServerDataPacket        = 1
	ServerExceptionPacket   = 2
	ServerProgressPacket    = 3
	ServerPongPacket        = 4
	ServerEndOfStreamPacket = 5
	ServerProfileInfoPacket = 6
	ServerTotalsPacket      = 7
	ServerExtremesPacket    = 8
)

const (
	DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES         = 50264
	DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS   = 51554
	DBMS_MIN_REVISION_WITH_BLOCK_INFO               = 51903
	DBMS_MIN_REVISION_WITH_CLIENT_INFO              = 54032
	DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE          = 54058
	DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO = 54060
)
