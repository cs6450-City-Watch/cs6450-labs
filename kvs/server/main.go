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
	puts uint64
	gets uint64
}

func (s *Stats) Sub(prev *Stats) Stats {
	r := Stats{}
	r.puts = s.puts - prev.puts
	r.gets = s.gets - prev.gets
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
	response  *kvs.Transaction_Response
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

func (kv *KVService) Commit(TransactionID int64) {
	// TODO
}

func (kv *KVService) Abort(TransactionID int64) {
	// TODO
}

// Accepts a transaction of 3 Put/Get operations. Returns responses for both operations
func (kv *KVService) Process_Transaction(request *kvs.Transaction_Request, response *kvs.Transaction_Response) error {
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

	kv.stats.puts += uint64(kvs.Transaction_size)

	response.Values = make([]string, kvs.Transaction_size)

	var i = 0
	for _, operation := range request.Data {
		if operation.IsRead {
			if value, found := kv.Get(operation.Key); found {
				response.Values[i] = value
			}

		} else {
			kv.Put(operation.Key, operation.Value, time.Duration(100*float64(time.Millisecond)))
		}
		i++
	}

	// Cache the response
	if request.TransactionID != 0 {
		kv.cacheMtx.Lock()
		kv.reqCache[request.TransactionID] = &RequestCache{
			response:  &kvs.Transaction_Response{Values: make([]string, kvs.Transaction_size)},
			timestamp: time.Now(),
		}
		copy(kv.reqCache[request.TransactionID].response.Values, response.Values)
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

	fmt.Printf("get/s %0.2f\nput/s %0.2f\nops/s %0.2f\n\n",
		float64(diff.gets)/deltaS,
		float64(diff.puts)/deltaS,
		float64(diff.gets+diff.puts)/deltaS)
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
