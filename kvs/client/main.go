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
			fmt.Println("Operation failed with abort:", resp.Value)
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
			// trOp.ForUpdate = true
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

func runConnection(wg *sync.WaitGroup, hosts []string, done *atomic.Bool, workload *kvs.Workload, totalOpsCompleted *uint64, clientID int, payments bool, ready *atomic.Bool) {
	fmt.Println("Starting connection", clientID)
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

	// ------ NORMAL WORKLOAD PATH ----------
	if !payments {
		// --- YCSB path (what you already had) ---
		for !done.Load() {
			ops, dests := buildTxn(workload, hosts)
			maxAttempts := 5
			for attempt := 0; attempt < maxAttempts; attempt++ {
				txID := client.Begin()
				if client.Prepare(ops, dests, txID) {
					client.Commit(ops, dests, txID)
					clientOpsCompleted += uint64(len(ops))
					break
				}
				client.Abort(ops, dests, txID)
			}
		}
		atomic.AddUint64(totalOpsCompleted, clientOpsCompleted)
		return
	}

	// -------- PAYMENTS PATH ----------
	// Map this goroutine to one of the 10 accounts
	src := clientID % 10
	dst := (src + 1) % 10

	// Only goroutine 0 initializes accounts once
	if clientID == 0 {
		initAccounts(&client, hosts) // sets all 10 accounts to "1000"
		ready.Store(true)            // signal everyone to start
		fmt.Println("Payments: accounts initialized; starting transfers.")
	} else {
		// Wait for init
		for !ready.Load() && !done.Load() {
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Run transfers; every 5 successful transfers, goroutine 0 audits
	succ := 0
	for !done.Load() {
		if runPaymentTxn(&client, hosts, src, dst) {
			succ++
			clientOpsCompleted += 4 // each payment txn does 2 reads + 2 writes in your helper

			if succ%5 == 0 && clientID == 0 {
				total, balances, ok := auditSumTxn(&client, hosts)
				if !ok {
					log.Println("Audit failed to read balances; will retry next time.")
				} else if total != 10000 {
					log.Fatalf("Invariant broken! total=%d balances=%v", total, balances)
				} else {
					fmt.Printf("Audit OK: total=%d balances=%v\n", total, balances)
				}
			}
		} else {
			// aborted (e.g., src had < $100); small backoff avoids hot spinning
			time.Sleep(2 * time.Millisecond)
		}
	}
	atomic.AddUint64(totalOpsCompleted, clientOpsCompleted) // TODO: only really accurate after at-least-once
}

//	--------------- CODE FOR TESTING --------------

// rpcGet returns (value, ok)
func (c *Client) rpcGet(txID int64, key string, forUpdate bool, hosts []string) (string, bool) {
	req := kvs.Operation_Request{TransactionID: txID,
		Op: kvs.Operation{Key: key, IsRead: true, ForUpdate: forUpdate}}
	dest := int(hashKey(key)) % len(hosts)
	var resp kvs.Operation_Response
	if err := c.hosts[dest].Call("KVService.Process_Operation", &req, &resp); err != nil {
		log.Fatal(err)
	}
	return resp.Value, resp.Success
}

func (c *Client) rpcPut(txID int64, key, val string, hosts []string) bool {
	req := kvs.Operation_Request{TransactionID: txID,
		Op: kvs.Operation{Key: key, Value: val, IsRead: false}}
	dest := int(hashKey(key)) % len(hosts)
	var resp kvs.Operation_Response
	if err := c.hosts[dest].Call("KVService.Process_Operation", &req, &resp); err != nil {
		log.Fatal(err)
	}
	return resp.Success
}

// Initialize 10 accounts to "1000" each (run once by clientID==0)
func initAccounts(c *Client, hosts []string) {
	tx := c.Begin()
	for i := 0; i < 10; i++ {
		if !c.rpcPut(tx, fmt.Sprintf("%d", i), "1000", hosts) {
			c.Abort(nil, nil, tx)
			log.Fatal("initAccounts: put failed, aborting")
		}
	}
	c.Commit(nil, nil, tx)
}

// One payment transfer: move $100 from src -> dst
func runPaymentTxn(c *Client, hosts []string, src, dst int) bool {
	tx := c.Begin()

	// X-lock both accounts up front (avoid upgrades)
	sVal, ok := c.rpcGet(tx, fmt.Sprintf("%d", src), true, hosts)
	if !ok {
		c.Abort(nil, nil, tx)
		return false
	}
	dVal, ok := c.rpcGet(tx, fmt.Sprintf("%d", dst), true, hosts)
	if !ok {
		c.Abort(nil, nil, tx)
		return false
	}

	// parse balances (default 0 if empty)
	sBal, dBal := 0, 0
	if sVal != "" {
		fmt.Sscanf(sVal, "%d", &sBal)
	}
	if dVal != "" {
		fmt.Sscanf(dVal, "%d", &dBal)
	}

	if sBal < 100 {
		c.Abort(nil, nil, tx)
		return false
	}

	// write both sides
	if !c.rpcPut(tx, fmt.Sprintf("%d", src), fmt.Sprintf("%d", sBal-100), hosts) {
		c.Abort(nil, nil, tx)
		return false
	}
	if !c.rpcPut(tx, fmt.Sprintf("%d", dst), fmt.Sprintf("%d", dBal+100), hosts) {
		c.Abort(nil, nil, tx)
		return false
	}

	c.Commit(nil, nil, tx)
	return true
}

func auditSumTxn(c *Client, hosts []string) (int, []int, bool) {
	tx := c.Begin()
	total := 0
	balances := make([]int, 10)
	for i := 0; i < 10; i++ {
		v, ok := c.rpcGet(tx, fmt.Sprintf("%d", i), false, hosts)
		if !ok {
			c.Abort(nil, nil, tx)
			return 0, nil, false
		}
		b := 0
		if v != "" {
			fmt.Sscanf(v, "%d", &b)
		}
		balances[i] = b
		total += b
	}
	c.Commit(nil, nil, tx)
	return total, balances, true
}

// -------------- End of Code for Testing ---------------

func runClient(id int, hosts []string, done *atomic.Bool, workload *kvs.Workload, numConnections int, resultsCh chan<- uint64, payments bool) {
	var wg sync.WaitGroup
	totalOpsCompleted := uint64(0)

	var ready atomic.Bool
	// instantiate waitgroup before goroutines
	for connId := 0; connId < numConnections; connId++ {
		wg.Add(1)
	}
	for connId := 0; connId < numConnections; connId++ {
		go runConnection(&wg, hosts, done, workload, &totalOpsCompleted, connId, payments, &ready)
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
	payments := flag.Bool("payments", false, "Run payment workload test (10 accounts)")

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
		runClient(clientId, hosts, &done, workload, *numConnections, resultsCh, *payments)
	}(clientId)

	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)

	opsCompleted := <-resultsCh

	elapsed := time.Since(start)

	opsPerSec := float64(opsCompleted) / elapsed.Seconds()
	fmt.Printf("throughput %.2f ops/s\n", opsPerSec)
}
