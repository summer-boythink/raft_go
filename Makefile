all:build

build:fmt
	go build -o ./build/server ./example/server/

run-server:build
	./build/server

fmt:
	go mod tidy && gofmt -w .