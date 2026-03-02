.PHONY: build test vet lint clean

build:
	go build ./...

test:
	go test -race -count=1 ./...

vet:
	go vet ./...

clean:
	rm -rf dist/
	rm -f ksink
