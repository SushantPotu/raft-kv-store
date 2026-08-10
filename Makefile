.PHONY: build test test-race lint proto-gen docker-build compose-up compose-down

build:
	go build ./...

test:
	go test ./...

test-race:
	go test -race ./...

lint:
	buf lint
	golangci-lint run ./... || true

proto-gen:
	buf generate

docker-build:
	docker build -f deploy/docker/Dockerfile.kvnode -t raft-kv-store/kvnode:dev .
	docker build -f deploy/docker/Dockerfile.kvrouter -t raft-kv-store/kvrouter:dev .
	docker build -f deploy/docker/Dockerfile.metaservice -t raft-kv-store/metaservice:dev .

compose-up:
	docker compose -f deploy/docker/docker-compose.yml up -d

compose-down:
	docker compose -f deploy/docker/docker-compose.yml down
