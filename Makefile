
build: generate
	go build -pgo=default.pgo ./cmd/s3fs

generate:
	go generate ./...
