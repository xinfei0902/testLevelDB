module testLevelDB

go 1.15

require (
	github.com/go-ini/ini v1.66.2
	github.com/golang/protobuf v1.4.3
	github.com/hyperledger/fabric v2.1.1+incompatible
	github.com/hyperledger/fabric-lib-go v1.0.0 // indirect
	github.com/hyperledger/fabric-protos-go v0.0.0-20211118165945-23d738fc3553
	github.com/pkg/errors v0.9.1
	github.com/sykesm/zap-logfmt v0.0.4 // indirect
	github.com/syndtr/goleveldb v1.0.1-0.20210305035536-64b5b1c73954
	github.com/tjfoc/gmsm v1.4.1 // indirect
	github.com/willf/bitset v1.1.10
	go.uber.org/zap v1.19.1 // indirect
	google.golang.org/grpc v1.43.0 // indirect
	google.golang.org/protobuf v1.25.0 // indirect
)

replace github.com/hyperledger/fabric => ../github.com/hyperledger/fabric
