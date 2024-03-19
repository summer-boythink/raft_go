## raft_go

This is a simple KV database based on `Raft` algorithm,**this implementation is very easy to understand**

## Usage

### build
```
make
```

### start-server
```sh
# node1 [leader]
./build/server --local http://127.0.0.1:8080 --peer http://127.0.0.1:8081 --peer http://127.0.0.1:8082

# node2
./build/server --local http://127.0.0.1:8081 --peer http://127.0.0.1:8082 --peer http://127.0.0.1:8080

# node3
./build/server --local http://127.0.0.1:8082 --peer http://127.0.0.1:8081 --peer http://127.0.0.1:8080
```

### start-client
```sh
# set key
./build/client set key1 val1 -a http://127.0.0.1:8080

# get key
./build/client get key1 -a http://127.0.0.1:8081

# rm key
./build/client rm key1 -a http://127.0.0.1:8080

```

## Future plan
1. I will change the `MemStorage` method to store the log on disk
2. I'm going to use `LSM trees` and other, more advanced data structures
3. Add tests