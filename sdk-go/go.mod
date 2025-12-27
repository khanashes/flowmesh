module github.com/flowmesh/sdk-go

go 1.24.0

require (
	github.com/flowmesh/engine v0.0.0
	google.golang.org/grpc v1.77.0
)

require (
	golang.org/x/net v0.47.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/text v0.31.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/flowmesh/engine => ../engine
