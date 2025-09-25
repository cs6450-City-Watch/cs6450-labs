package kvs

const Transaction_size = 3

type Operation_Request struct {
	TransactionID int64
	Op            Operation
}

type Operation_Response struct {
	Value string
}

type Operation struct {
	Key    string
	Value  string
	IsRead bool
}
