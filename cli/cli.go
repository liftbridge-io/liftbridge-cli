package cli

import (
	"context"
	"fmt"
	"time"

	lift "github.com/liftbridge-io/go-liftbridge"
	liftApi "github.com/liftbridge-io/liftbridge-api/go"
	"github.com/urfave/cli/v2"
)

const (
	activityStreamName = "__activity"
	// TODO: this could be a parameter.
	timeoutDuration     = 3 * time.Second
	defaultStreamName   = "some-stream"
	defaultMessageValue = "some-value"
)

var (
	// TODO: allow specifying multiple addresses.
	addressFlag = &cli.StringFlag{
		Name:    "address",
		Aliases: []string{"a"},
		Usage:   "connect to the endpoint specified by `ADDRESS`",
		Value:   "127.0.0.1:9292",
	}
	streamFlag = &cli.StringFlag{
		Name:    "stream",
		Aliases: []string{"s"},
		Usage:   "use `STREAM`",
		Value:   defaultStreamName,
	}
	// TODO: allow specifying multiple messages.
	messageFlag = &cli.StringFlag{
		Name:    "message",
		Aliases: []string{"m"},
		Usage:   "send a message with a string `VALUE`",
		Value:   defaultMessageValue,
	}
	createStreamFlag = &cli.BoolFlag{
		Name:    "create-stream",
		Aliases: []string{"c"},
		Usage:   "create the stream if it doesn't exist",
	}

	subscribeCommand = &cli.Command{
		Name:    "subscribe",
		Aliases: []string{"s"},
		Usage:   "Subscribes to a stream",
		Action:  subscribe,
		Flags: []cli.Flag{
			createStreamFlag,
			streamFlag,
		},
	}
	subscribeActivityStreamCommand = &cli.Command{
		Name:    "subscribe-activity-stream",
		Aliases: []string{"sas"},
		Usage:   "Subscribes to the activity stream",
		Action:  subscribeActivityStream,
		Flags: []cli.Flag{
			streamFlag,
		},
	}
	publishCommand = &cli.Command{
		Name:    "publish",
		Aliases: []string{"p"},
		Usage:   "Publishes to a stream",
		Action:  publish,
		Flags: []cli.Flag{
			messageFlag,
			createStreamFlag,
			streamFlag,
		},
	}
	metadataCommand = &cli.Command{
		Name:    "metadata",
		Aliases: []string{"m"},
		Usage:   "Fetches metadata",
		Action:  metadata,
	}
)

func connectToEndpoint(address string) (lift.Client, error) {
	client, err := lift.Connect([]string{address})
	if err != nil {
		return nil, fmt.Errorf("connection failed with address %v: %w", address, err)
	}

	return client, nil
}

// subscribeToStream subscribes to a channel and blocks until an error occurs.
func subscribeToStream(
	streamName string,
	handler func(*lift.Message),
	endPointAddress string,
	createStream bool,
) error {
	client, err := connectToEndpoint(endPointAddress)
	if err != nil {
		return fmt.Errorf("stream subscription failed: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	if createStream {
		// TODO: allow specifying the subject name.
		err = client.CreateStream(ctx, streamName+"subject", streamName)
		if err != nil && err != lift.ErrStreamExists {
			return fmt.Errorf("stream creation failed for stream %v: %w", streamName, err)
		}
	}

	errC := make(chan error)

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	err = client.Subscribe(ctx, streamName, func(m *lift.Message, err error) {
		if err != nil {
			errC <- err
			return
		}

		handler(m)
		// TODO: allow setting subscription options.
	}, lift.StartAtEarliestReceived())
	if err != nil {
		return fmt.Errorf("stream subscription failed for stream %v: %w", streamName, err)
	}

	return <-errC
}

func subscribe(c *cli.Context) error {
	streamName := c.String(streamFlag.Name)

	return subscribeToStream(streamName, func(message *lift.Message) {
		fmt.Printf("Received message with data: %v, offset: %v\n", string(message.Value()), message.Offset())
	}, c.String(addressFlag.Name), c.Bool(createStreamFlag.Name))
}

func subscribeActivityStream(c *cli.Context) error {
	return subscribeToStream(activityStreamName, func(message *lift.Message) {
		var se liftApi.ActivityStreamEvent
		err := se.Unmarshal(message.Value())
		if err != nil {
			fmt.Printf("Received an invalid activity message from the activity stream: %v\n", err.Error())
			return
		}

		var activityStr string

		switch se.Op {
		case liftApi.ActivityStreamOp_CREATE_STREAM:
			op := se.CreateStreamOp
			activityStr = fmt.Sprintf("stream: %v, partitions: %v", op.Stream, op.Partitions)
		case liftApi.ActivityStreamOp_DELETE_STREAM:
			op := se.DeleteStreamOp
			activityStr = fmt.Sprintf("stream: %v", op.Stream)
		case liftApi.ActivityStreamOp_PAUSE_STREAM:
			op := se.PauseStreamOp
			activityStr = fmt.Sprintf("stream: %v, partitions: %v, resumeAll: %v", op.Stream, op.Partitions, op.ResumeAll)
		case liftApi.ActivityStreamOp_RESUME_STREAM:
			op := se.ResumeStreamOp
			activityStr = fmt.Sprintf("stream: %v, partitions: %v", op.Stream, op.Partitions)
		default:
			activityStr = "unknown activity"
		}

		fmt.Printf("Received activity stream message: op: %v, %v, offset: %v\n",
			se.Op,
			activityStr,
			message.Offset(),
		)
	}, c.String(addressFlag.Name), c.Bool(createStreamFlag.Name))
}

func publish(c *cli.Context) error {
	client, err := connectToEndpoint(c.String(addressFlag.Name))
	if err != nil {
		return fmt.Errorf("publication failed: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	streamName := c.String(streamFlag.Name)

	if c.Bool(createStreamFlag.Name) {
		// TODO: allow specifying the subject name.
		err = client.CreateStream(ctx, streamName+"subject", streamName)
		if err != nil && err != lift.ErrStreamExists {
			return fmt.Errorf("stream creation failed for stream %v: %w", streamName, err)
		}
	}

	data := []byte(c.String(messageFlag.Name))

	_, err = client.Publish(
		ctx,
		streamName,
		data,
		// TODO: allow setting another ACK policy.
		lift.AckPolicyLeader(),
	)
	if err != nil && err != lift.ErrStreamExists {
		return fmt.Errorf("publication failed: %w", err)
	}

	return nil
}

func brokerString(b *lift.BrokerInfo) string {
	return fmt.Sprintf("%v (%v)", b.ID(), b.Addr())
}

func metadata(c *cli.Context) error {
	client, err := connectToEndpoint(c.String(addressFlag.Name))
	if err != nil {
		return fmt.Errorf("metadata fetching failed: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	metadata, err := client.FetchMetadata(ctx)
	if err != nil {
		return fmt.Errorf("metadata fetching failed: %w", err)
	}

	// TODO: allow other output formats.
	fmt.Printf("addresses:\n")
	for _, addr := range metadata.Addrs() {
		fmt.Printf(" %v\n", addr)
	}
	fmt.Printf("brokers:\n")
	for _, broker := range metadata.Brokers() {
		fmt.Printf(" %v\n", brokerString(broker))
	}
	fmt.Printf("last updated:\n %v\n", metadata.LastUpdated())

	fmt.Printf("streams:\n")
	for sk, sv := range metadata.Streams() {
		fmt.Printf(" %v\n", sk)
		fmt.Printf("  partitions:\n")
		for pk, pv := range sv.Partitions() {
			fmt.Printf("   %v (ID %v)\n", pk, pv.ID())
			fmt.Printf("    leader:\n     %v\n", brokerString(pv.Leader()))
			fmt.Printf("    ISRs:\n")
			for _, isr := range pv.ISR() {
				fmt.Printf("     %v\n", brokerString(isr))
			}
			fmt.Printf("    replicas:\n")
			for _, isr := range pv.Replicas() {
				fmt.Printf("     %v\n", brokerString(isr))
			}
		}
	}

	return nil
}

func Run(args []string) error {
	app := &cli.App{
		Name:  "Liftbridge Command Line Interface",
		Usage: "allows making requests to a Liftbridge server",
		Flags: []cli.Flag{
			addressFlag,
		},
		Commands: []*cli.Command{
			subscribeCommand,
			subscribeActivityStreamCommand,
			publishCommand,
			metadataCommand,
		},
	}

	return app.Run(args)
}
