up:
	@docker compose up -d
down:
	@docker compose down

cli:
	docker run -it --rm --net clickhouse-go_clickhouse --link clickhouse:clickhouse-server yandex/clickhouse-client --host clickhouse-server

test:
	go install -race -v
	go test -i -v
	go test -race -timeout 30s -v .
	go test -race -timeout 30s -v ./tests/...

staticcheck:
	staticcheck ./...

codegen:
	@cd lib/column && go run codegen/main.go

coverage:
	go test -coverprofile=coverage.out -v .
	go tool cover -html=coverage.out
