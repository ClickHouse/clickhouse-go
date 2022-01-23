up:
	@docker compose up -d
down:
	@docker compose down

cli:
	docker run -it --rm --net clickhouse-go_clickhouse --link clickhouse:clickhouse-server yandex/clickhouse-client --host clickhouse-server

test:
	go install -race -v
	go test -race -timeout 30s -v .
	go test -race -timeout 30s -v ./tests/...

lint:
	golangci-lint run || :
	gocritic check -disable=singleCaseSwitch ./... || :

staticcheck:
	staticcheck ./...

codegen:
	@cd lib/column && go run codegen/main.go
