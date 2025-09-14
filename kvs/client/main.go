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
	rpcClient *rpc.Client
}

func Dial(addr string) *Client {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	return &Client{rpcClient}
}

func generateRequestID() int64 {
	var bytes [8]byte
	_, err := rand.Read(bytes[:])
	if err != nil {
		log.Fatal("Failed to generate random request ID:", err)
	}
	return int64(binary.LittleEndian.Uint64(bytes[:]))
}

// Sends a batch of RPC calls synchronously with retry logic
func (client *Client) Send_Synch_Batch(putData []kvs.BatchOperation) []string {
	requestID := generateRequestID()
	request := kvs.Batch_Request{
		RequestID: requestID,
		Data:      putData,
	}

	const maxRetries = 3
	const baseDelay = 100 * time.Millisecond

	for attempt := range maxRetries {
		response := kvs.Batch_Response{}
		err := client.rpcClient.Call("KVService.Process_Batch", &request, &response)
		if err == nil {
			return response.Values
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

	return nil // unreachable
}

func hashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func runConnection(wg *sync.WaitGroup, hosts []string, done *atomic.Bool, workload *kvs.Workload, totalOpsCompleted *uint64) {
	defer wg.Done()

	// Dial all hosts
	clients := make([]*Client, len(hosts))
	for i, host := range hosts {
		clients[i] = Dial(host)
	}

	value := strings.Repeat("x", 128)
	const batchSize = 5000 //1024
	clientOpsCompleted := uint64(0)

	for !done.Load() {
		// Create batches for each server
		requests := make([][]kvs.BatchOperation, len(hosts))

		// organize work from workload
		for j := 0; j < batchSize; j++ {
			// XXX: something may go awry here when the total number of "yields"
			// from workload.Next() is not a clean multiple of batchSize.
			op := workload.Next()
			key := fmt.Sprintf("%d", op.Key)

			// Hash key to determine which server
			serverIndex := int(hashKey(key)) % len(hosts)
			batchRequestData := requests[serverIndex]
			var batchOp kvs.BatchOperation

			if op.IsRead {
				batchOp.Key = key
				batchOp.Value = ""
				batchOp.IsRead = true
			} else {
				batchOp.Key = key
				batchOp.Value = value
				batchOp.IsRead = false
			}
			batchRequestData = append(batchRequestData, batchOp)
			requests[serverIndex] = batchRequestData
			clientOpsCompleted++
		}

		// Send batches to each server
		for i := 0; i < len(hosts); i++ {
			batchRequestData := requests[i]
			if len(batchRequestData) > 0 {
				clients[i].Send_Synch_Batch(batchRequestData)
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
