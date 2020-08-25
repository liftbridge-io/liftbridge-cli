module main

go 1.14

require (
	github.com/liftbridge-io/go-liftbridge v1.0.0
	github.com/liftbridge-io/liftbridge-api v1.1.0
	github.com/urfave/cli/v2 v2.2.0
	google.golang.org/grpc v1.31.0 // indirect
)

// Temporary replacement until https://github.com/liftbridge-io/go-liftbridge/pull/71 has
// been merged.
replace github.com/liftbridge-io/go-liftbridge => github.com/ably-forks/go-liftbridge v1.0.1-0.20200825093129-8f9c1e088be7
