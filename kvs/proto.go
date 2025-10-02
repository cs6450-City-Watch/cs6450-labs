package kvs

const Transaction_size = 3

type Operation_Request struct {
	TransactionID int64
	Op            Operation
}

type Operation_Response struct {
	Value   string
	Success bool
}

type Operation struct {
	Key    string
	Value  string
	IsRead bool
	// default false
	ForUpdate bool
}

type PhaseTwoCommit struct {
	TransactionID int64
	Lead          bool
}
