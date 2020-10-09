module main

go 1.14

require (
	github.com/liftbridge-io/go-liftbridge v1.0.1-0.20200901163447-38f14d24c90d
	github.com/liftbridge-io/go-liftbridge/v2 v2.0.2-0.20201002231334-dd0ae5b3c895
	github.com/liftbridge-io/liftbridge v1.2.0
	github.com/liftbridge-io/liftbridge-api v1.1.1-0.20201003172511-d603f70cc6a1
	github.com/nats-io/nats-server/v2 v2.1.4
	github.com/sirupsen/logrus v1.6.0
	github.com/stretchr/testify v1.4.0
	github.com/urfave/cli/v2 v2.2.0
)

replace github.com/liftbridge-io/liftbridge => github.com/ably-forks/liftbridge v1.9.3-0.20201009095248-2d5d2a8f0cf4
