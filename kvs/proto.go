package kvs

const Transaction_size = 3

type Operation_Request struct {
	TransactionID int64 // should probably change this to match others
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

type Commit_Query struct {
	TransactionID  int64
	ClientID       uint64
	TransactionIDX int16 // TODO: is this necessary?
}

type Commit_Query_Response struct {
	TransactionID  int64
	ClientID       uint64
	TransactionIDX int16 // TODO: is this necessary?
	IsAbort        bool
}

type Commit_Imperative struct {
	TransactionID  int64
	ClientID       uint64
	TransactionIDX int16 // TODO: is this necessary?
	IsAbort        bool
	Lead           bool
}

type Commit_Imperative_Response struct {
	TransactionID  int64
	ClientID       uint64
	TransactionIDX int16 // TODO: is this necessary?
	IsAbort        bool
}

