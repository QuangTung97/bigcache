.PHONY: lint test install-tools escape

lint:
	$(foreach f,$(shell go fmt ./...),@echo "Forgot to format file: ${f}"; exit 1;)
	go vet ./...
	revive -config revive.toml -formatter friendly ./...

test:
	go test -v -race -count=1 ./...

install-tools:
	go install github.com/mgechev/revive

escape:
	go build -gcflags '-m -m  -l' .
