/*
A single client issues and completes a transaction (with a unique ID) sent to this server. It does not send the next get/put until the previous one is done.
Thus, there is no concurrency within a transaction.
*/
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	// for profiling
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"

	"github.com/rstutsman/cs6450-labs/kvs"
)

type Stats struct {
	commits uint64
	aborts  uint64
	puts    uint64
	gets    uint64
}

func (s *Stats) Sub(prev *Stats) Stats {
	r := Stats{}
	r.puts = s.puts - prev.puts
	r.gets = s.gets - prev.gets
	r.commits = s.commits - prev.commits
	r.aborts = s.aborts - prev.aborts
	return r
}

type LockKind int

const (
	None LockKind = iota
	ReadLock
	WriteLock
)

type Entry struct {
	sync.RWMutex
	Value string
}

type TxState struct {
	writeAheadMap map[string]string
	lockedKeys    map[string]LockKind
}

// Thread-safe transaction table (no manual locking needed)
type TxTable struct {
	data sync.Map // key: int64 (unique transaction ID), value: *TxState
}

type KVService struct {
	// primarily used to create missing entries or logs stats

	sync.RWMutex // used for directory operations only

	// Stats
	stats     Stats
	prevStats Stats
	lastPrint time.Time

	txs  TxTable
	data map[string]*Entry // per-key entry pointers
}

func NewKVService() *KVService {
	kvs := &KVService{
		data: make(map[string]*Entry),
	}
	kvs.lastPrint = time.Now()
	return kvs
}

// ensure creates entry if missing, using KVService lock only for directory operations
func (s *KVService) ensure(key string) *Entry {
	// TODO: optimize locking here
	s.Lock()
	defer s.Unlock()
	if e, ok := s.data[key]; ok {
		return e
	}
	e := s.data[key]
	if e == nil {
		e = &Entry{}
		s.data[key] = e
	}
	return e
}

// func fnvHash(data string) uint32 {
// 	const prime = 16777619
// 	hash := uint32(2166136261)

// 	for i := 0; i < len(data); i++ {
// 		hash ^= uint32(data[i])
// 		hash *= prime
// 	}

// 	return hash
// }

// func (kv *KVService) GetShardIndex(key string) int {
// 	hash := fnvHash(key)
// 	return int(hash) % len(kv.shards)
// }

// Transactional Get method
func (kv *KVService) Get(txID int64, key string, forUpdate bool) (string, bool) {
	kv.Lock()
	kv.stats.gets++
	kv.Unlock()

	// Load transaction
	v, ok := kv.txs.data.Load(txID)
	if !ok {
		// TODO: should panic maybe rather than return
		return "unknown transaction ID", false // unknown tx
	}

	// type assertion b/c sync.Map stores empty interface
	tx := v.(*TxState)

	// Check WAL first (read-your-writes)
	if val, ok := tx.writeAheadMap[key]; ok {
		return val, true
	}

	// Ensure entry exists
	e := kv.ensure(key)

	// Acquire appropriate lock based on for_update flag
	switch tx.lockedKeys[key] {
	case None:
		if forUpdate {
			if !e.TryLock() {
				return "abort because of no wait deadlock", false
			}
			tx.lockedKeys[key] = WriteLock
		} else {
			if !e.TryRLock() {
				return "abort because of no wait deadlock", false
			}
			tx.lockedKeys[key] = ReadLock
		}

	case ReadLock:
		if forUpdate {
			// No-wait 2PL: we don't upgrade; fail fast so client aborts & retries taking X up front
			return "abort because of no wait deadlock - can't upgrade a readlock (forUpdate read)", false
		}
		// already have S; proceed to read

	case WriteLock:
		// already exclusive; proceed
	}
	// If already WriteLock, can skip additional locking as already have exclusive access

	// Read safely
	return e.Value, true
}

// Transactional Put method
func (kv *KVService) Put(txID int64, key, value string) (string, bool) {
	kv.Lock()
	kv.stats.puts++
	kv.Unlock()

	// Load transaction
	v, ok := kv.txs.data.Load(txID)
	if !ok {
		// TODO: should panic rather than return
		return "unknown transaction ID", false // invalid tx
	}
	tx := v.(*TxState)

	// Ensure entry exists
	e := kv.ensure(key)

	// Acquire WriteLock - abort if any conflict
	switch tx.lockedKeys[key] {
	case None:
		// no wait deadlock prevention
		if !e.TryLock() {
			return "abort because of no wait deadlock", false // abort: key is already locked
		}

		tx.lockedKeys[key] = WriteLock
	case ReadLock:
		// fmt.Println("UPGRADE ATTEMPT ABORTED")
		return "abort because of no wait deadlock - can't upgrade read lock on write, key: " + key, false // abort: key is already locked
	case WriteLock:
		// already exclusive
	}

	// Record in WAL
	tx.writeAheadMap[key] = value
	return value, true
}

func (kv *KVService) Begin(txID *int64, response *struct{}) error {
	// Check if transaction already exists
	if _, exists := kv.txs.data.Load(*txID); exists {
		return nil // already active
	}

	// Create and store new transaction
	tx := &TxState{
		writeAheadMap: make(map[string]string),
		lockedKeys:    make(map[string]LockKind),
	}
	kv.txs.data.Store(*txID, tx)
	return nil
}

func (kv *KVService) CanCommit(txID *int64, response *bool) error {
	// If transaction data is not present, return false.
	// Kind of redundant after our checks during operations,
	// but nice to have regardless.
	// Gets around ugly situations with the `ok` check in `Commit` and `Abort`.
	_, ok := kv.txs.data.Load(txID)
	*response = ok
	return nil
}

func (kv *KVService) Commit(msg *kvs.PhaseTwoCommit, response *struct{}) error {
	if msg.Lead { // don't duplicate commit counts
		kv.Lock()
		kv.stats.commits++
		kv.Unlock()
	}

	// Load transaction
	v, ok := kv.txs.data.Load(msg.TransactionID)
	if !ok {
		// at this point if something fails it's golang's own damn fault
		fmt.Printf("\n\n(!) SOMETHING WEIRD HAPPENED, TELL ASH\n\n")
		return nil
	}
	tx := v.(*TxState)

	// Skipping nil guards below because there is no delete operation, and if value is in lockedKeys or writeaheadMap, it must exist in data because ensure is called before either set

	// Apply WAL changes under held locks
	for k, v := range tx.writeAheadMap {
		kv.RLock() // reading from map, so need RLock on map even thought we hold locks on entries
		e := kv.data[k]
		kv.RUnlock()
		e.Value = v
	}

	// Release all held locks
	for k, kind := range tx.lockedKeys {
		kv.RLock()
		e := kv.data[k]
		kv.RUnlock()

		switch kind {
		case WriteLock:
			e.Unlock()
		case ReadLock:
			e.RUnlock()
		}
	}

	// Delete transaction
	kv.txs.data.Delete(msg.TransactionID)
	return nil
}

func (kv *KVService) Abort(msg *kvs.PhaseTwoCommit, response *struct{}) error {
	if msg.Lead { // don't duplicate abort counts
		kv.Lock()
		kv.stats.aborts++
		kv.Unlock()
	}

	// Load transaction
	v, ok := kv.txs.data.Load(msg.TransactionID)
	if !ok {
		// at this point if something fails it's golang's own damn fault
		fmt.Printf("\n\n(!) SOMETHING WEIRD HAPPENED, TELL ASH\n\n")
		return nil
	}
	tx := v.(*TxState)

	// Release all held locks (skip WAL apply)
	for k, kind := range tx.lockedKeys {
		kv.RLock()
		e := kv.data[k]
		kv.RUnlock()

		switch kind {
		case WriteLock:
			e.Unlock()
		case ReadLock:
			e.RUnlock()
		}
	}

	// Delete transaction
	kv.txs.data.Delete(msg.TransactionID)
	return nil
}

// Accepts a single Put/Get operation. Returns a response
func (kv *KVService) Process_Operation(request *kvs.Operation_Request, response *kvs.Operation_Response) error {
	// Check this request ID in the cache
	// if request.TransactionID != 0 {
	// 	kv.cacheMtx.RLock()
	// 	cachedResponse, exists := kv.reqCache[request.TransactionID]
	// 	kv.cacheMtx.RUnlock()

	// 	if exists {
	// 		*response = *cachedResponse.response
	// 		return nil
	// 	}
	// }

	// kv.stats.puts += uint64(kvs.Transaction_size)

	operation := request.Op
	txID := request.TransactionID

	if operation.IsRead {
		// if value, found := kv.Get(txID, operation.Key); found {
		// 	response.Value = value
		// }
		// fmt.Println("GET", operation.Key)
		response.Value, response.Success = kv.Get(txID, operation.Key, operation.ForUpdate)

	} else {
		// fmt.Println("PUT", operation.Key, operation.Value)
		response.Value, response.Success = kv.Put(txID, operation.Key, operation.Value)
	}

	// // Cache the response
	// if request.TransactionID != 0 {
	// 	kv.cacheMtx.Lock()
	// 	kv.reqCache[request.TransactionID] = &RequestCache{
	// 		response:  &kvs.Operation_Response{},
	// 		timestamp: time.Now(),
	// 	}
	// 	kv.reqCache[request.TransactionID].response.Value = response.Value
	// 	kv.cacheMtx.Unlock()
	// }

	return nil
}

func (kv *KVService) printStats() {
	kv.Lock()
	stats := kv.stats
	prevStats := kv.prevStats
	kv.prevStats = stats
	now := time.Now()
	lastPrint := kv.lastPrint
	kv.lastPrint = now
	kv.Unlock()

	diff := stats.Sub(&prevStats)
	deltaS := now.Sub(lastPrint).Seconds()

	// NOTE: commits and aborts will print on all servers
	fmt.Printf("get/s %0.2f\nput/s %0.2f\nops/s %0.2f\ncommits/s %0.2f\naborts/s %0.2f\n\n",
		float64(diff.gets)/deltaS,
		float64(diff.puts)/deltaS,
		float64(diff.gets+diff.puts)/deltaS,
		float64(diff.commits)/deltaS,
		float64(diff.aborts)/deltaS,
	)
}

func main() {
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile in specified directory")
	port := flag.String("port", "8080", "Port to run the server on")

	flag.Parse()

	// cpuprofile flag set to log profiling data
	// ONLY when flag is set.
	if *cpuprofile != "" {
		hostname, err := os.Hostname()
		if err != nil {
			log.Fatal(err)
		}
		f, err := os.Create(fmt.Sprintf("%s/%s", *cpuprofile, hostname))
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()

		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
		go func() {
			<-sigc
			pprof.StopCPUProfile()
			os.Exit(0)
		}()
	}

	kvs := NewKVService()
	rpc.Register(kvs)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", fmt.Sprintf(":%v", *port))
	if e != nil {
		log.Fatal("listen error:", e)
	}

	fmt.Printf("Starting KVS server on :%s\n", *port)

	go func() {
		for {
			kvs.printStats()
			time.Sleep(1 * time.Second)
		}
	}()

	http.Serve(l, nil)
}
