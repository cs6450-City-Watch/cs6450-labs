package main

import (
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

type Client struct {
	transactionID uint64
	writeSet      map[string]string // TODO: Currently unusued. Read canvas for hints
	hosts         []*rpc.Client
}

func Dial(addr string) *rpc.Client {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	return rpcClient
}

// Todo: change ID generation logic for serializability
func generateTransactionID() int64 {
	var bytes [8]byte
	_, err := rand.Read(bytes[:])
	if err != nil {
		log.Fatal("Failed to generate random transaction ID:", err)
	}
	return int64(binary.LittleEndian.Uint64(bytes[:]))
}

func (client *Client) Commit(operations [kvs.Transaction_size]kvs.Operation, destinations [kvs.Transaction_size]int, transactionID int64) {
	// TODO
}

func (client *Client) Abort(operations [kvs.Transaction_size]kvs.Operation, destinations [kvs.Transaction_size]int, transactionID int64) {
	// TODO
}

func (client *Client) Begin() int64 {
	transactionID := generateTransactionID()

	// TODO: Other logic that has to be in Begin()

	return transactionID
}

// Sends an operation that is a part of a transaction with retry logic
func (client *Client) Prepare(operations [kvs.Transaction_size]kvs.Operation, destinations [kvs.Transaction_size]int, transactionID int64) string {
	for i := 0; i < kvs.Transaction_size; i++ {
		op := operations[i]
		request := kvs.Operation_Request{
			TransactionID: transactionID,
			Op:            op,
		}

		const maxRetries = 3
		const baseDelay = 100 * time.Millisecond

		for attempt := range maxRetries {
			// TODO: Right now a client always sends messages to the same host!!!
			target_server := client.hosts[destinations[i]]
			response := kvs.Operation_Response{}
			err := target_server.Call("KVService.Process_Operation", &request, &response)
			if err == nil {
				return response.Value
			}

			// Log retry attempt
			if attempt < maxRetries-1 {
				delay := baseDelay * time.Duration(1<<attempt) // delay *= 2
				log.Printf("RPC call failed (attempt %d/%d): %v, retrying in %v", attempt+1, maxRetries, err, delay)
				time.Sleep(delay)
			} else {
				log.Fatal("RPC call failed after all retries:", err)
			}
		}

		// TODO
		return "nil" // unreachable
	}
	return "success?" // TODO
}

func hashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func runConnection(wg *sync.WaitGroup, hosts []string, done *atomic.Bool, workload *kvs.Workload, totalOpsCompleted *uint64) {
	defer wg.Done()

	// First, build a client.
	client := Client{}
	participants := make([]*rpc.Client, len(hosts))
	for i, host := range hosts {
		participants[i] = Dial(host) // Fix: use participants, not clients
	}
	client.hosts = participants

	value := strings.Repeat("x", 128)
	clientOpsCompleted := uint64(0)

	for !done.Load() {
		// Oficially begin the transaction
		transactionID := client.Begin()

		requests := [kvs.Transaction_size]kvs.Operation{}
		destinations := [kvs.Transaction_size]int{}

		// Process each operation in a transaction
		for j := 0; j < kvs.Transaction_size; j++ {
			// XXX: something may go awry here when the total number of "yields"
			// from workload.Next() is not a clean multiple of transaction_size.
			op := workload.Next()
			key := fmt.Sprintf("%d", op.Key)

			// Hash key to determine which server.
			serverIndex := int(hashKey(key)) % len(hosts)

			var trOp kvs.Operation

			if op.IsRead {
				trOp.Key = key
				trOp.Value = ""
				trOp.IsRead = true
			} else {
				trOp.Key = key
				trOp.Value = value
				trOp.IsRead = false
			}
			requests[j] = trOp
			destinations[j] = serverIndex
			clientOpsCompleted++
		}

		// Execute transaction without commmit
		if len(requests) > 0 {
			client.Prepare(requests, destinations, transactionID)
		}

		// Commit/Abort logic
		if 1 == 1 {
			// Commit transaction
			if len(requests) > 0 {
				client.Commit(requests, destinations, transactionID)
			}
		}
	}
	atomic.AddUint64(totalOpsCompleted, clientOpsCompleted) // TODO: only really accurate after at-least-once
}

func runClient(id int, hosts []string, done *atomic.Bool, workload *kvs.Workload, numConnections int, resultsCh chan<- uint64) {
	var wg sync.WaitGroup
	totalOpsCompleted := uint64(0)

	// instantiate waitgroup before goroutines
	for connId := 0; connId < numConnections; connId++ {
		wg.Add(1)
	}
	for connId := 0; connId < numConnections; connId++ {
		go runConnection(&wg, hosts, done, workload, &totalOpsCompleted)
	}

	fmt.Println("waiting for connections to finish")
	wg.Wait()
	fmt.Printf("Client %d finished operations.\n", id)
	resultsCh <- totalOpsCompleted
}

type HostList []string

func (h *HostList) String() string {
	return strings.Join(*h, ",")
}

func (h *HostList) Set(value string) error {
	*h = strings.Split(value, ",")
	return nil
}

func main() {
	hosts := HostList{}

	flag.Var(&hosts, "hosts", "Comma-separated list of host:ports to connect to")
	theta := flag.Float64("theta", 0.99, "Zipfian distribution skew parameter")
	workload := flag.String("workload", "YCSB-B", "Workload type (YCSB-A, YCSB-B, YCSB-C)")
	secs := flag.Int("secs", 30, "Duration in seconds for each client to run")
	numConnections := flag.Int("connections", 1, "Number of connections per client")

	flag.Parse()

	if len(hosts) == 0 {
		hosts = append(hosts, "localhost:8080")
	}

	fmt.Printf(
		"hosts %v\n"+
			"theta %.2f\n"+
			"workload %s\n"+
			"secs %d\n"+
			"connections %d\n",
		hosts, *theta, *workload, *secs, *numConnections,
	)

	start := time.Now()

	done := atomic.Bool{}
	resultsCh := make(chan uint64)

	clientId := 0
	go func(clientId int) {
		workload := kvs.NewWorkload(*workload, *theta)
		runClient(clientId, hosts, &done, workload, *numConnections, resultsCh)
	}(clientId)

	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)

	opsCompleted := <-resultsCh

	elapsed := time.Since(start)

	opsPerSec := float64(opsCompleted) / elapsed.Seconds()
	fmt.Printf("throughput %.2f ops/s\n", opsPerSec)
}
