ping:
	tinygo build -opt=0 -ldflags="-X main.name=pinger -X main.version=v0.1.0" -o pinger.wasm -target wasi pinger.go

pong:
	tinygo build -opt=0 -ldflags="-X main.name=ponger -X main.version=v0.1.0" -o ponger.wasm -target wasi ponger.go
