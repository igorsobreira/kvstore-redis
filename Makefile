
test:
	@go test -race -i
	@go test -race -v

lint:
	@golint `find . -name "*.go"`

fmt:
	@go fmt ./...
