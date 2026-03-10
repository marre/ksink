.PHONY: build test vet lint clean

build:
	go build ./...

test:
	go test -race -count=1 ./...

vet:
	go vet ./...

lint:
	golangci-lint run ./...

clean:
	rm -rf dist/
	rm -f ksink
