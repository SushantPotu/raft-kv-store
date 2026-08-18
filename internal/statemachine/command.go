// Package statemachine holds the KV command wire format proposed to
// raft.Node.Propose (Command/CommandOp/CommandResult, this file) and the
// real raft.StateMachine implementation that decodes it against a real
// internal/storage/engine.Engine (adapter.go).
//
// These types used to live unexported inside internal/server (as
// command/commandOp/commandResult), which worked fine when the only
// consumers were KVServer.propose (encoding) and internal/server's own
// fakeStateMachine.Apply (decoding) — both in the same package. Once a real
// StateMachine needed to decode the exact same format from outside
// internal/server, the types had to move somewhere both sides could import,
// hence this package. The wire format itself is unchanged.
package statemachine

// CommandOp identifies which KVService operation a proposed log entry
// encodes.
type CommandOp string

const (
	OpPut CommandOp = "put"
	OpDel CommandOp = "delete"
	OpCAS CommandOp = "cas"
)

// Command is the payload proposed to raft.Node.Propose for every KVService
// write. RequestID lets the caller correlate a committed/applied entry back
// to the client request that produced it.
type Command struct {
	RequestID     string    `json:"request_id"`
	Op            CommandOp `json:"op"`
	Key           []byte    `json:"key"`
	Value         []byte    `json:"value,omitempty"`
	ExpectedValue []byte    `json:"expected_value,omitempty"`
	ExpectAbsent  bool      `json:"expect_absent,omitempty"`
}

// CommandResult is what a raft.StateMachine.Apply implementation returns
// (JSON-encoded, as the []byte result raft.StateMachine.Apply's signature
// allows) for a given Command. Put/Delete leave Swapped/ActualValue/Found
// unused.
type CommandResult struct {
	Swapped     bool   `json:"swapped,omitempty"`
	ActualValue []byte `json:"actual_value,omitempty"`
	Found       bool   `json:"found,omitempty"`
}
