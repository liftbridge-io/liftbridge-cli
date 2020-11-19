package cli

import (
	"context"
	"fmt"
	"time"

	lift "github.com/liftbridge-io/go-liftbridge/v2"
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
	subjectFlag = &cli.StringFlag{
		Name:        "subject",
		Aliases:     []string{"u"},
		Usage:       "subject name to use when creating the stream",
		DefaultText: "same as the stream name",
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
	readonlyFlag = &cli.BoolFlag{
		Name:    "readonly",
		Aliases: []string{"r"},
		Usage:   "set the stream as readonly",
		Value:   true,
	}
	resumeAllFlag = &cli.BoolFlag{
		Name:    "resume-all",
		Aliases: []string{"r"},
		Usage:   "resume all partitions if one of them is published to instead of resuming only that partition",
	}
	partitionsFlag = &cli.IntSliceFlag{
		Name:    "partitions",
		Aliases: []string{"p"},
		Usage:   "targeted partitions",
	}

	createCommand = &cli.Command{
		Name:    "create",
		Aliases: []string{"c"},
		Usage:   "Creates a stream",
		Action:  create,
		Flags: []cli.Flag{
			streamFlag,
			subjectFlag,
		},
	}
	subscribeCommand = &cli.Command{
		Name:    "subscribe",
		Aliases: []string{"s"},
		Usage:   "Subscribes to a stream",
		Action:  subscribe,
		Flags: []cli.Flag{
			createStreamFlag,
			streamFlag,
			subjectFlag,
		},
	}
	subscribeActivityStreamCommand = &cli.Command{
		Name:    "subscribe-activity-stream",
		Aliases: []string{"sas"},
		Usage:   "Subscribes to the activity stream",
		Action:  subscribeActivityStream,
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
			subjectFlag,
		},
	}
	setReadonlyCommand = &cli.Command{
		Name:    "set-readonly",
		Aliases: []string{"r"},
		Usage:   "Sets a stream as readonly",
		Action:  setReadonly,
		Flags: []cli.Flag{
			createStreamFlag,
			streamFlag,
			subjectFlag,
			readonlyFlag,
			partitionsFlag,
		},
	}
	pauseCommand = &cli.Command{
		Name:    "pause",
		Aliases: []string{"u"},
		Usage:   "Pauses a stream",
		Action:  pause,
		Flags: []cli.Flag{
			createStreamFlag,
			streamFlag,
			subjectFlag,
			resumeAllFlag,
			partitionsFlag,
		},
	}
	deleteCommand = &cli.Command{
		Name:    "delete",
		Aliases: []string{"d"},
		Usage:   "Deletes a stream",
		Action:  delete,
		Flags: []cli.Flag{
			createStreamFlag,
			streamFlag,
			subjectFlag,
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

func ensureStreamCreated(ctx context.Context, client lift.Client, streamName, subjectName string) error {
	if len(subjectName) == 0 {
		subjectName = streamName
	}

	err := client.CreateStream(ctx, subjectName, streamName)
	if err != nil && err != lift.ErrStreamExists {
		return fmt.Errorf("stream creation failed for stream %v: %w", streamName, err)
	}

	return nil
}

// subscribeToStream subscribes to a channel and blocks until an error occurs.
func subscribeToStream(
	streamName, subjectName string,
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
		if err := ensureStreamCreated(ctx, client, streamName, subjectName); err != nil {
			return err
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

func create(c *cli.Context) error {
	client, err := connectToEndpoint(c.String(addressFlag.Name))
	if err != nil {
		return fmt.Errorf("creation failed: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	streamName := c.String(streamFlag.Name)
	subjectName := c.String(subjectFlag.Name)

	if len(subjectName) == 0 {
		subjectName = streamName
	}

	err = client.CreateStream(ctx, subjectName, streamName)
	if err != nil {
		return fmt.Errorf("stream creation failed for stream %v: %w", streamName, err)
	}

	return nil
}

func subscribe(c *cli.Context) error {
	streamName := c.String(streamFlag.Name)
	subjectName := c.String(subjectFlag.Name)

	return subscribeToStream(streamName, subjectName, func(message *lift.Message) {
		fmt.Printf("Received message with data: %v, offset: %v\n", string(message.Value()), message.Offset())
	}, c.String(addressFlag.Name), c.Bool(createStreamFlag.Name))
}

func subscribeActivityStream(c *cli.Context) error {
	return subscribeToStream(activityStreamName, "", func(message *lift.Message) {
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
	}, c.String(addressFlag.Name), false)
}

func publish(c *cli.Context) error {
	client, err := connectToEndpoint(c.String(addressFlag.Name))
	if err != nil {
		return fmt.Errorf("publication failed: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	streamName := c.String(streamFlag.Name)
	subjectName := c.String(subjectFlag.Name)

	if c.Bool(createStreamFlag.Name) {
		if err := ensureStreamCreated(ctx, client, streamName, subjectName); err != nil {
			return err
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

func intToInt32Slice(slice []int) []int32 {
	result := make([]int32, 0, len(slice))
	for _, value := range slice {
		result = append(result, int32(value))
	}
	return result
}

func setReadonly(c *cli.Context) error {
	client, err := connectToEndpoint(c.String(addressFlag.Name))
	if err != nil {
		return fmt.Errorf("set readonly failed: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	streamName := c.String(streamFlag.Name)
	subjectName := c.String(subjectFlag.Name)

	if c.Bool(createStreamFlag.Name) {
		if err := ensureStreamCreated(ctx, client, streamName, subjectName); err != nil {
			return err
		}
	}

	readonly := c.Bool(readonlyFlag.Name)
	partitions := c.IntSlice(partitionsFlag.Name)
	err = client.SetStreamReadonly(
		ctx,
		streamName,
		lift.Readonly(readonly),
		lift.ReadonlyPartitions(intToInt32Slice(partitions)...),
	)
	if err != nil {
		return fmt.Errorf("set readonly failed: %w", err)
	}

	return nil
}

func pause(c *cli.Context) error {
	client, err := connectToEndpoint(c.String(addressFlag.Name))
	if err != nil {
		return fmt.Errorf("pause failed: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	streamName := c.String(streamFlag.Name)
	subjectName := c.String(subjectFlag.Name)

	if c.Bool(createStreamFlag.Name) {
		if err := ensureStreamCreated(ctx, client, streamName, subjectName); err != nil {
			return err
		}
	}

	var opts []lift.PauseOption
	if c.Bool(resumeAllFlag.Name) {
		opts = append(opts, lift.ResumeAll())
	}

	partitions := c.IntSlice(partitionsFlag.Name)
	opts = append(opts, lift.PausePartitions(intToInt32Slice(partitions)...))

	err = client.PauseStream(
		ctx,
		streamName,
		opts...,
	)
	if err != nil {
		return fmt.Errorf("pause failed: %w", err)
	}

	return nil
}

func delete(c *cli.Context) error {
	client, err := connectToEndpoint(c.String(addressFlag.Name))
	if err != nil {
		return fmt.Errorf("delete failed: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	streamName := c.String(streamFlag.Name)
	subjectName := c.String(subjectFlag.Name)

	if c.Bool(createStreamFlag.Name) {
		if err := ensureStreamCreated(ctx, client, streamName, subjectName); err != nil {
			return err
		}
	}

	err = client.DeleteStream(
		ctx,
		streamName,
	)
	if err != nil {
		return fmt.Errorf("delete failed: %w", err)
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
			createCommand,
			subscribeCommand,
			subscribeActivityStreamCommand,
			publishCommand,
			setReadonlyCommand,
			pauseCommand,
			deleteCommand,
			metadataCommand,
		},
	}

	return app.Run(args)
}
