package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func Connect() *clientv3.Client {
	// This function returns a client connection to an etcd node

	hosts := [][]string{[]string{"etcd0:2379"}, []string{"etcd1:2379"}, []string{"etcd2:2379"}}
	host := random.RandomChoice(hosts)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   host,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to connect to etcd: %v", err)
		// Antithesis Assertion: client should always be able to connect to an etcd host
		assert.Unreachable("Client failed to connect to an etcd host", map[string]interface{}{"host": host, "error": err})
		os.Exit(1)
	}
	return cli
}

func TestSequentialConsistency() {
	// This function will:
	// 1. Create a key
	// 2. Randomly connect to a node and read the key before writing an update.
	// 3. Ensure that all read values are == the last successfully-written value
        //
        // This directly tests the guarantees of
        // https://etcd.io/docs/v3.3/learning/api_guarantees/.
        //
        // If a write is complete and the API indicates as such, then no matter which
        // client we connect to thereafter, we should read that value.
        //
        // Note: we're not concurrently writing and reading, and so not testing
        // linearizability.

	ctx := context.Background()

        // Initialization
	k := "sequential-consistency-test-key"
	cli := Connect()
	_, err := cli.Put(ctx, k, "0")
	// Antithesis Assertion: A failed request is OK since we expect them to happen.
	assert.Sometimes(err == nil, "Client can initialize sequential consistency test successfully", map[string]interface{}{"error": err})
	cli.Close()
	if err != nil {
		log.Printf("Client failed to initialize sequential-consistency test successfully", err)
		os.Exit(0)
	}

	last_success := "0"
	for i := 1; i <= 1000; i++ {
		cli = Connect()

		get_resp, err := cli.Get(ctx, k)
		// Antithesis Assertion: sometimes get requests are successful. A failed request is OK since we expect them to happen.
		assert.Sometimes(err == nil, "Client can make successful get requests", map[string]interface{}{"error": err})
		if err != nil {
			log.Printf("Client failed to get key %s: %v", k, err)
			continue
		} else {
			assert.Always(string(get_resp.Kvs[0].Value) == last_success, "Client's read should always reflect the latest completed write", map[string]interface{}{"last_read": string(get_resp.Kvs[0].Value), "expecting": last_success})
		}

		_, err = cli.Put(ctx, "sequential-consistency-test-key", strconv.Itoa(i))
		// Antithesis Assertion: A failed request is OK since we expect them to happen.
		assert.Sometimes(err == nil, "Client can initialize sequential consistency test successfully", map[string]interface{}{"error": err})
		if err != nil {
			log.Printf("Client failed to write to key %s: %v", k, err)
			continue
		} else {
			last_success = strconv.Itoa(i)
		}

		cli.Close()
	}

	assert.Reachable("Completion of a sequential consistency check", nil)
	log.Printf("Completion of a sequential consistency check")
}

func main() {
	TestSequentialConsistency()
}
