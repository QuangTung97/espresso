.PHONY: lint test

test:
	go test -v ./...

lint:
	go fmt ./...
	golint ./...
	go vet ./...
	errcheck ./...