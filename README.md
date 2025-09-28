# Useful commands

ssh node[num] 
exit to leave

ip addr

./run-cluster.sh 1 1 "" "-asynch"

nproc - number of cores

./run-cluster.sh 1 1 "" "-connections 10 -payments true"

# notes
runcluster script autmaitcally if give num servers will make the remaining one's clients. also as client arg it will give all the server ip:ports as hosts which ends up in hostlist.