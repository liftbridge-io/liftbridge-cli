package cli

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	lift "github.com/liftbridge-io/go-liftbridge/v2"
	"github.com/liftbridge-io/liftbridge/server"
	natsdTest "github.com/nats-io/nats-server/v2/test"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var storagePath string

func init() {
	tmpDir, err := ioutil.TempDir("", "liftbridge_cli_test_")
	if err != nil {
		panic(fmt.Errorf("Error creating temp dir: %v", err))
	}
	if err := os.Remove(tmpDir); err != nil {
		panic(fmt.Errorf("Error removing temp dir: %v", err))
	}
	storagePath = tmpDir
}

func cleanupStorage(t *testing.T) {
	err := os.RemoveAll(storagePath)
	require.NoError(t, err)
}

func getTestConfig(id string) *server.Config {
	config := server.NewDefaultConfig()
	config.Clustering.RaftBootstrapSeed = true
	config.DataDir = filepath.Join(storagePath, id)
	config.Clustering.RaftSnapshots = 1
	config.LogRaft = true
	config.Clustering.ServerID = id
	config.LogLevel = uint32(log.DebugLevel)
	config.NATS.Servers = []string{"nats://localhost:4222"}
	config.LogSilent = true
	config.Port = 0
	return config
}

func runServerWithConfig(t *testing.T, config *server.Config) *server.Server {
	server, err := server.RunServerWithConfig(config)
	require.NoError(t, err)
	t.Cleanup(func() {
		server.Stop()
	})
	return server
}

func getMetadataLeader(t *testing.T, timeout time.Duration, servers ...*server.Server) *server.Server {
	var (
		leader   *server.Server
		deadline = time.Now().Add(timeout)
	)
	for time.Now().Before(deadline) {
		for _, s := range servers {
			if !s.IsRunning() {
				continue
			}
			if s.IsLeader() {
				if leader != nil {
					t.Fatalf("Found more than one metadata leader")
				}
				leader = s
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if leader == nil {
		t.Fatalf("No metadata leader found")
	}
	return leader
}

func setupTest(t *testing.T) ([]string, lift.Client) {
	t.Cleanup(func() {
		defer cleanupStorage(t)
	})

	// Use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	t.Cleanup(func() {
		ns.Shutdown()
	})

	// Configure server.
	config := getTestConfig("a")
	server := runServerWithConfig(t, config)

	// Wait to elect self as leader.
	getMetadataLeader(t, 10*time.Second, server)

	args := os.Args[0:1]
	serverAddress := net.JoinHostPort("localhost", strconv.Itoa(server.GetListenPort()))
	args = append(args,
		"--"+addressFlag.Name, serverAddress,
	)

	// Connect a client.
	client, err := lift.Connect([]string{serverAddress})
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Close()
	})

	return args, client
}

// TestPublication tests if a message can be published.
func TestPublication(t *testing.T) {
	args, client := setupTest(t)

	streamName := "test-stream"
	messageText := "test message text"

	args = append(args,
		publishCommand.Name,
		"--"+createStreamFlag.Name,
		"--"+streamFlag.Name, streamName,
		"--"+messageFlag.Name, messageText,
	)

	err := Run(args)
	require.NoError(t, err)

	// Check that the message has been published.
	msgs := make(chan *lift.Message, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	err = client.Subscribe(ctx, streamName, func(msg *lift.Message, err error) {
		require.NoError(t, err)
		msgs <- msg
		cancel()
	}, lift.StartAtLatestReceived())
	require.NoError(t, err)

	// Wait to get the new message.
	select {
	case msg := <-msgs:
		require.Equal(t, messageText, string(msg.Value()))
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive expected message")
	}
}

// TestPublicationNonExisting checks that a message cannot be published on a non
// existing stream if the stream creation flag is not set.
func TestPublicationNonExisting(t *testing.T) {
	args, _ := setupTest(t)

	streamName := "test-stream"
	messageText := "test message text"

	args = append(args,
		publishCommand.Name,
		"--"+streamFlag.Name, streamName,
		"--"+messageFlag.Name, messageText,
	)

	err := Run(args)
	require.Error(t, err)
	require.True(t, strings.HasSuffix(err.Error(), lift.ErrNoSuchPartition.Error()))
}
