.PHONY: lint test install-tools

lint:
	go fmt ./...
	go vet ./...
	revive -config revive.toml -formatter friendly ./...

test:
	go test -v -tags integration ./...


install-tools:
	go install github.com/matryer/moq
	go install github.com/mgechev/revive
