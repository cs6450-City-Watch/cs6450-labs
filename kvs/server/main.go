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
}

func (s *Stats) Sub(prev *Stats) Stats {
	r := Stats{}
	r.commits = s.commits - prev.commits
	r.aborts = s.aborts - prev.aborts
	return r
}

// sharding ---------------------------------
type KeyValue struct {
	Value      string
	Expiration time.Time // simulates TTL
}
type Shard struct {
	data map[string]KeyValue
	mu   sync.RWMutex
}

type RequestCache struct {
	response  *kvs.Operation_Response
	timestamp time.Time
}

type KVService struct {
	sync.Mutex
	shards    []*Shard
	replicas  int
	stats     Stats
	prevStats Stats
	lastPrint time.Time
	reqCache  map[int64]*RequestCache
	cacheMtx  sync.RWMutex
}

func NewKVService(numShards, numReplicas int) *KVService {
	kvs := &KVService{}
	kvs.shards = make([]*Shard, numShards)
	kvs.replicas = numReplicas
	kvs.reqCache = make(map[int64]*RequestCache)

	for i := 0; i < numShards; i++ {
		kvs.shards[i] = &Shard{data: make(map[string]KeyValue)}
	}

	kvs.lastPrint = time.Now()
	return kvs
}

func fnvHash(data string) uint32 {
	const prime = 16777619
	hash := uint32(2166136261)

	for i := 0; i < len(data); i++ {
		hash ^= uint32(data[i])
		hash *= prime
	}

	return hash
}

func (kv *KVService) GetShardIndex(key string) int {
	hash := fnvHash(key)
	return int(hash) % len(kv.shards)
}

func (kv *KVService) IncrementCommits() {
	kv.Lock()
	defer kv.Unlock()
	kv.stats.commits++
}
func (kv *KVService) IncrementAborts() {
	kv.Lock()
	defer kv.Unlock()
	kv.stats.aborts++
}

// single getter method
// not I/O bound, just a helper per request
func (kv *KVService) Get(key string) (string, bool) {
	shardIndex := kv.GetShardIndex(key)
	shard := kv.shards[shardIndex]
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	item, ok := shard.data[key]
	if !ok || time.Now().After(item.Expiration) {
		return "", false
	}

	return item.Value, true
}

// single putter method
// not I/O bound, just a helper per request
func (kv *KVService) Put(key, value string, ttl time.Duration) {
	shardIndex := kv.GetShardIndex(key)
	shard := kv.shards[shardIndex]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	expiration := time.Now().Add(ttl)
	shard.data[key] = KeyValue{Value: value, Expiration: expiration}
}

func (kv *KVService) Prepare_Commit(query *kvs.Commit_Query, response *kvs.Commit_Query_Response) error {
	// TODO
	// current idea:
	// `Commit_Query` should identify some relevant lock.
	// We inspect that lock, see if it's held,
	// and if it is, we vote no.
	// If it isn't, we vote yes.
	//
	// If there's any interface I can take advantage of after 2PL reasoning,
	// I'd appreciate it, if only for consistency's sake.
	response.TransactionID = query.TransactionID
	response.ClientID = query.TransactionID
	response.TransactionIDX = query.TransactionIDX

	// TODO: decide on vote...
	response.IsAbort = false
	return nil
}

func (kv *KVService) Do_Abort(query *kvs.Commit_Query, response *kvs.Commit_Query_Response) error {
	// TODO
	// current idea:
	// `Commit_Query` should identify all actions in the write-ahead cache
	// and we can clear those.
	// Alternatively we have a cache relevant to the transaction as a whole.
	// Either way, we make sure everything relevant is cleared,
	// and then send an acknowledgement.
	// Results may vary with other threads trying to acquire locks.
	//
	// If there's any interface I can take advantage of after 2PL reasoning,
	// I'd appreciate it, if only for consistency's sake.
	if query.Lead {
		kv.IncrementAborts()
	}
	response.TransactionID = query.TransactionID
	response.ClientID = query.TransactionID
	response.TransactionIDX = query.TransactionIDX
	response.IsAbort = query.IsAbort

	// TODO: alter state

	return nil
}

func (kv *KVService) Do_Commit(query *kvs.Commit_Query, response *kvs.Commit_Query_Response) error {
	// TODO
	// current idea:
	// `Commit_Query` should identify all actions in the write-ahead cache
	// and we can commit those.
	// Alternatively we have a cache relevant to the transaction as a whole.
	// Either way, we make sure everything relevant is committed,
	// and then send an acknowledgement.
	// Results may vary with other threads trying to acquire locks.
	//
	// If there's any interface I can take advantage of after 2PL reasoning,
	// I'd appreciate it, if only for consistency's sake.
	if query.Lead {
		kv.IncrementCommits()
	}
	response.TransactionID = query.TransactionID
	response.ClientID = query.TransactionID
	response.TransactionIDX = query.TransactionIDX
	response.IsAbort = query.IsAbort

	// TODO: alter state

	return nil
}

// Accepts a single Put/Get operation. Returns a response
func (kv *KVService) Process_Operation(request *kvs.Operation_Request, response *kvs.Operation_Response) error {
	// Check this request ID in the cache
	if request.TransactionID != 0 {
		kv.cacheMtx.RLock()
		cachedResponse, exists := kv.reqCache[request.TransactionID]
		kv.cacheMtx.RUnlock()

		if exists {
			*response = *cachedResponse.response
			return nil
		}
	}

	// kv.stats.puts += uint64(kvs.Transaction_size) // did this ever work?

	operation := request.Op

	if operation.IsRead {
		if value, found := kv.Get(operation.Key); found {
			response.Value = value
		}

	} else {
		kv.Put(operation.Key, operation.Value, time.Duration(100*float64(time.Millisecond)))
	}

	// Cache the response
	if request.TransactionID != 0 {
		kv.cacheMtx.Lock()
		kv.reqCache[request.TransactionID] = &RequestCache{
			response:  &kvs.Operation_Response{},
			timestamp: time.Now(),
		}
		kv.reqCache[request.TransactionID].response.Value = response.Value
		kv.cacheMtx.Unlock()
	}

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

	fmt.Printf("commit/s %0.2f\nabort/s %0.2f\nops/s %0.2f (note this last metric is relative)\n\n",
		float64(diff.commits)/deltaS,
		float64(diff.aborts)/deltaS,
		float64(diff.commits+diff.aborts)/deltaS)
}

func main() {
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile in specified directory")
	port := flag.String("port", "8080", "Port to run the server on")
	numShards := flag.Int("numshards", 10, "number of shards to use")
	numReplicas := flag.Int("numreplicas", 10, "number of replicas")

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

	kvs := NewKVService(*numShards, *numReplicas)
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
