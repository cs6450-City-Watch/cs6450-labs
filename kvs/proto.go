package kvs

const Transaction_size = 3

type Transaction_Request struct {
	TransactionID int64
	Data          [Transaction_size]Operation
}

type Transaction_Response struct {
	Values []string
}

type Operation struct {
	Key    string
	Value  string
	IsRead bool
}
