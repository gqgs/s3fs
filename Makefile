
build: generate
	go build ./cmd/s3fs

generate:
	go generate ./...
