CLICKHOUSE_VERSION ?= latest
CLICKHOUSE_TEST_TIMEOUT ?= 600s
CLICKHOUSE_QUORUM_INSERT ?= 1
COMPOSE_PROJECT_NAME ?= clickhouse-go

up:
	@docker ps -aqf "name=^/clickhouse$$" | xargs -r docker rm -f
	@COMPOSE_PROJECT_NAME=$(COMPOSE_PROJECT_NAME) docker compose up --wait --remove-orphans
down:
	@COMPOSE_PROJECT_NAME=$(COMPOSE_PROJECT_NAME) docker compose down

up-cluster:
	@COMPOSE_PROJECT_NAME=$(COMPOSE_PROJECT_NAME) docker compose -f docker-compose.cluster.yml up --force-recreate --remove-orphans

down-cluster:
	@COMPOSE_PROJECT_NAME=$(COMPOSE_PROJECT_NAME) docker compose -f docker-compose.cluster.yml down

cli:
	docker run -it --rm --net clickhouse-go_clickhouse --link clickhouse:clickhouse-server --host clickhouse-server

test:
	@go install -race -v
	@CLICKHOUSE_VERSION=$(CLICKHOUSE_VERSION) CLICKHOUSE_QUORUM_INSERT=$(CLICKHOUSE_QUORUM_INSERT) go test -race -timeout $(CLICKHOUSE_TEST_TIMEOUT) -count=1 -v ./...

lint:
	golangci-lint run || :

fmt:
	@gofmt -w -l .

fmt-check:
	@out=$$(gofmt -l .); \
		if [ -n "$$out" ]; then \
			echo "The following files are not gofmt-formatted:"; \
			echo "$$out"; \
			echo "Run 'make fmt' to fix."; \
			exit 1; \
		fi

staticcheck:
	staticcheck ./...

codegen:
	@go run lib/column/codegen/main.go

.PHONY: fmt fmt-check
