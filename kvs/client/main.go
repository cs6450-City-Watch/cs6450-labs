package main

import (
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
	clientID  uint
	txCounter uint64
	hosts     []*rpc.Client
}

func (c *Client) nextTxID() int64 {
	n := atomic.AddUint64(&c.txCounter, 1)
	// Embed clientID in the upper 16 bits, txCounter in the lower 48 bits
	id := (uint64(c.clientID) << 48) | (n & ((1 << 48) - 1))
	return int64(id)
}

// helper to broadcast a method call to all hosts. This is used for Commit and Abort.
func (c *Client) broadcastMethod(method string, txID int64) {
	for _, host := range c.hosts {
		// use dummy reply struct
		if err := host.Call("KVService."+method, &txID, &struct{}{}); err != nil {
			log.Fatal("RPC call failed:", err)
		}
	}
}

func Dial(addr string) *rpc.Client {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	return rpcClient
}

// // Todo: Embed clientID into a transactionID
// func generateTransactionID(clientID uint) int64 {
// 	var bytes [8]byte
// 	_, err := rand.Read(bytes[:])
// 	if err != nil {
// 		log.Fatal("Failed to generate random transaction ID:", err)
// 	}
// 	return int64(binary.LittleEndian.Uint64(bytes[:]))
// }

func (client *Client) Commit(operations []kvs.Operation, destinations []int, transactionID int64) {
	client.broadcastMethod("Commit", transactionID)
}

func (client *Client) Abort(operations []kvs.Operation, destinations []int, transactionID int64) {
	client.broadcastMethod("Abort", transactionID)
}

func (client *Client) Begin() int64 {
	transactionID := client.nextTxID()
	client.broadcastMethod("Begin", transactionID)
	return transactionID
}

// Sends an operation that is a part of a transaction
func (client *Client) Prepare(operations []kvs.Operation, destinations []int, transactionID int64) bool {
	for i := 0; i < len(operations); i++ {
		request := kvs.Operation_Request{
			TransactionID: transactionID,
			Op:            operations[i],
		}

		// get correct server
		server := client.hosts[destinations[i]]

		resp := kvs.Operation_Response{}
		if err := server.Call("KVService.Process_Operation", &request, &resp); err != nil {
			log.Fatal("RPC call failed:", err)
		}
		if !resp.Success { // Abort the transaction if any operation fails
			return false
		}
	}
	return true
}

func hashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

// transaction for original workload
func buildTxn(workload *kvs.Workload, hosts []string) ([]kvs.Operation, []int) {
	value := strings.Repeat("x", 128) // just a dummy value

	operations := [kvs.Transaction_size]kvs.Operation{}
	destinations := [kvs.Transaction_size]int{}

	// Process each operation in a transaction
	for j := 0; j < kvs.Transaction_size; j++ {
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
		operations[j] = trOp
		destinations[j] = serverIndex
	}
	return operations[:], destinations[:]
}

func runConnection(wg *sync.WaitGroup, hosts []string, done *atomic.Bool, workload *kvs.Workload, totalOpsCompleted *uint64, clientID int) {
	defer wg.Done()

	// First, build a client.
	client := Client{
		clientID:  uint(clientID),
		txCounter: 0,
	}
	// TODO: Generate client ID

	participants := make([]*rpc.Client, len(hosts))
	for i, host := range hosts {
		participants[i] = Dial(host) // Fix: use participants, not clients
	}
	client.hosts = participants

	clientOpsCompleted := uint64(0)

	for !done.Load() {
		ops, dests := buildTxn(workload, hosts)

		// attempt until prepared successfully
		for {
			txID := client.Begin()
			if client.Prepare(ops, dests, txID) {
				client.Commit(ops, dests, txID)
				clientOpsCompleted += uint64(len(ops))
				break
			}
			client.Abort(ops, dests, txID)
			// loop repeats with a NEW txID from Begin()
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
		go runConnection(&wg, hosts, done, workload, &totalOpsCompleted, connId)
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
