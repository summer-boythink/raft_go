all:build

build:fmt
	go build -o ./build/server ./example/server/ &&
	go build -o ./build/client ./example/client/

run-server:build
	./build/server

fmt:
	go mod tidy && gofmt -w .