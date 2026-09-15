package raftcore

import "github.com/SushantPotu/raft-kv-store/pkg/raft"

// raftLog is Raft Core's in-memory view over the replicated log. It is not
// itself durable: per the Ready-loop contract in docs/adr/0001, the caller
// persists Ready.Entries into raft.Storage before the next Ready() is
// produced, and this type's "stable" bookkeeping simply assumes that has
// happened by then. Reads that need the durable log (recovery on startup)
// go through raft.Storage; everything else operates on the in-memory copy
// kept here so Node's hot path never has to round-trip through Storage.
//
// Snapshotting/compaction is a later workstream (see snapshot.go), so this
// implementation keeps the whole log in memory from index 1 onward. base
// tracks the logical index immediately before entries[0] (0 unless a
// snapshot has been applied), so the arithmetic here would still make sense
// once compaction lands, even though nothing here creates a snapshot yet.
type raftLog struct {
	entries     []raft.LogEntry // entries[k] has Index == base+1+k
	base        raft.LogIndex   // index just before entries[0]; 0 if nothing compacted
	baseTerm    raft.Term       // term of the entry at `base` (0 unless restored from a snapshot)
	stableIndex raft.LogIndex   // last index this Node assumes is already durable in Storage
}

// newRaftLog loads whatever raft.Storage already has (the crash-recovery
// path) into an in-memory raftLog.
func newRaftLog(storage raft.Storage) *raftLog {
	l := &raftLog{}

	first, err := storage.FirstIndex()
	if err != nil {
		first = 1
	}
	last, err := storage.LastIndex()
	if err != nil {
		last = 0
	}
	l.base = first - 1
	if last > l.base {
		if ents, err := storage.Entries(first, last+1, 0); err == nil {
			l.entries = append(l.entries, ents...)
		}
	}
	l.stableIndex = last
	return l
}

func (l *raftLog) lastIndex() raft.LogIndex {
	return l.base + raft.LogIndex(len(l.entries))
}

// termAt returns the term of the entry at logical index i. Index 0 always
// has term 0 (the "nothing before the start of the log" sentinel used for
// prevLogIndex/prevLogTerm comparisons).
func (l *raftLog) termAt(i raft.LogIndex) (raft.Term, bool) {
	if i == 0 {
		return 0, true
	}
	if i == l.base {
		return l.baseTerm, true
	}
	if i <= l.base || i > l.lastIndex() {
		return 0, false
	}
	return l.entries[i-l.base-1].Term, true
}

// entriesFrom returns a copy of every entry with Index >= lo.
func (l *raftLog) entriesFrom(lo raft.LogIndex) []raft.LogEntry {
	if lo <= l.base {
		lo = l.base + 1
	}
	if lo > l.lastIndex() {
		return nil
	}
	start := lo - l.base - 1
	out := make([]raft.LogEntry, len(l.entries)-int(start))
	copy(out, l.entries[start:])
	return out
}

// appendNew assigns sequential indices (starting right after the current
// tail) to freshly proposed entries and appends them. Used only by the
// leader for its own locally-originated entries (client proposals, and the
// no-op it appends on election) — never for entries arriving over
// AppendEntries, which already carry explicit indices and go through
// truncateAndAppend instead.
func (l *raftLog) appendNew(entries []raft.LogEntry) []raft.LogEntry {
	idx := l.lastIndex()
	out := make([]raft.LogEntry, len(entries))
	for i, e := range entries {
		idx++
		e.Index = idx
		out[i] = e
	}
	l.entries = append(l.entries, out...)
	return out
}

// truncateAndAppend merges a batch of entries received via AppendEntries
// into the log, implementing Raft's log-matching / conflicting-tail rule
// (paper Figure 2, AppendEntries RPC receiver implementation, point 3): if
// an existing entry conflicts with a new one (same index, different term),
// delete the existing entry and everything after it, then append the new
// entries starting there. Entries already present with a matching term are
// left alone (idempotent under resends).
func (l *raftLog) truncateAndAppend(newEntries []raft.LogEntry) {
	for i, e := range newEntries {
		if existingTerm, ok := l.termAt(e.Index); ok && existingTerm == e.Term {
			continue
		}
		l.truncateTo(e.Index - 1)
		l.entries = append(l.entries, newEntries[i:]...)
		if e.Index-1 < l.stableIndex {
			// The tail we just overwrote may have previously been reported
			// as "stable" (already durable). It no longer is — the caller
			// must re-persist from here, which unstableEntries() below
			// will surface on the next Ready().
			l.stableIndex = e.Index - 1
		}
		return
	}
}

// truncateTo drops every entry with Index > idx.
func (l *raftLog) truncateTo(idx raft.LogIndex) {
	if idx >= l.lastIndex() {
		return
	}
	if idx <= l.base {
		l.entries = nil
		return
	}
	l.entries = l.entries[:idx-l.base]
}

// unstableEntries returns the entries not yet handed off for persistence.
func (l *raftLog) unstableEntries() []raft.LogEntry {
	return l.entriesFrom(l.stableIndex + 1)
}

// isUpToDate implements the RequestVote "at least as up-to-date" check
// (paper §5.4.1): compare last log term first, then last log index.
func (l *raftLog) isUpToDate(lastIdx raft.LogIndex, lastTerm raft.Term) bool {
	myLastIdx := l.lastIndex()
	myLastTerm, _ := l.termAt(myLastIdx)
	if lastTerm != myLastTerm {
		return lastTerm > myLastTerm
	}
	return lastIdx >= myLastIdx
}

// conflictHint computes the (conflict_index, conflict_term) hints a
// follower returns on AppendEntries rejection, per the fast-backtracking
// optimization described in the Raft paper's §5.3 discussion (and detailed
// in the "Students' Guide to Raft"): if we don't have prevIdx at all,
// point the leader at the end of our log; otherwise point it at the first
// index of the conflicting term so the leader can skip its own entire
// batch of entries from that term in one round-trip.
func (l *raftLog) conflictHint(prevIdx raft.LogIndex) (raft.LogIndex, raft.Term) {
	last := l.lastIndex()
	if prevIdx > last {
		return last + 1, 0
	}
	term, ok := l.termAt(prevIdx)
	if !ok || term == 0 {
		return l.base + 1, 0
	}
	idx := prevIdx
	for idx > l.base+1 {
		t, _ := l.termAt(idx - 1)
		if t != term {
			break
		}
		idx--
	}
	return idx, term
}

// lastIndexWithTerm scans backward for the highest index whose term equals
// term, used by the leader to turn a follower's conflict_term hint into a
// concrete nextIndex (Figure 2 fast-backtrack optimization). Terms are
// non-decreasing with index, so the scan can stop as soon as it passes
// below term.
func (l *raftLog) lastIndexWithTerm(term raft.Term) (raft.LogIndex, bool) {
	for i := l.lastIndex(); i > l.base; i-- {
		t, _ := l.termAt(i)
		if t == term {
			return i, true
		}
		if t < term {
			break
		}
	}
	return 0, false
}
