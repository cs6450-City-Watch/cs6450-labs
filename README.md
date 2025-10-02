# Results

## Final throughput numbers

1 client and 1 server: total `TODO`

2 clients and 2 servers: total `TODO`

4 clients and 4 servers: total `TODO`

## Hardware utilization metrics

pprof results:

```
TODO
```

## Scaling characteristics 

## Analysis on the effects of contention

# Design

## Successful Ideas

## Failed Ideas

# Reproducibility
Our experiments were done on 8 CloudLab m510 machines.

run `./run-cluster.sh <server_count> <client_count> "-numshards <#>" "-connections <#>" "-payments <true/false>"`

For example:

`./run-cluster.sh 1 1 "-connections 70 -payments true"`

Runs the "payments" workload testing the correctness of our 2PC/2PL approach.

`-connections` is how many goroutines each client machine runs. For optimal performance we suggest doing an exponential grid search followed by linear finetuning.

`-payments true` runs a special payments workload as described in assignment.

# Reflections

When attempting to achieve higher performance via client-side multithreading, adding additional routines did not produce expected performance improvement. This is due to lock contention and 2PC aborts for higher number of clients, which is expected. 

Individual contributions from each team member:

- Artem: 
- Ash: 
- Brendon: 
- Cayden: 