all:build

build:fmt
	go build -o ./build/server ./example/server/
	go build -o ./build/client ./example/client/

run-server:build
	./build/server --local http://127.0.0.1:8080 --peer http://127.0.0.1:8081 --peer http://127.0.0.1:8082

run-client-set:build
	./build/client set key1 val1 -a http://127.0.0.1:8080

run-client-get:build
	./build/client get key1 -a http://127.0.0.1:8081

fmt:
	go mod tidy && gofmt -w .