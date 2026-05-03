package scan

import "testing"

func TestEnqueueRemoteHeaderCandidateRequiresContinuousParent(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.flow.taskRuntime.InsertTreeHeader(makeTestHeader(10, "0x0a", ""))

	before := env.taskPool.Stats().EnqueuedHeaderHash
	ok := env.flow.EnqueueRemoteHeaderCandidate("0x0b", "0x0b", "0x0a", "0xb")
	after := env.taskPool.Stats().EnqueuedHeaderHash

	if !ok {
		t.Fatal("expected continuous remote header candidate to enqueue header-by-hash")
	}
	if after != before+1 {
		t.Fatalf("expected one header-by-hash task, before=%d after=%d", before, after)
	}
	if env.blockTree.Get("0x0b") != nil {
		t.Fatal("remote header candidate must not be inserted into blocktree directly")
	}
}

func TestEnqueueRemoteHeaderCandidateRejectsInvalidContinuity(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.flow.taskRuntime.InsertTreeHeader(makeTestHeader(10, "0x0a", ""))

	cases := []struct {
		name       string
		blockHash  string
		headerHash string
		parentHash string
		number     string
	}{
		{
			name:       "hash mismatch",
			blockHash:  "0x0b",
			headerHash: "0x0c",
			parentHash: "0x0a",
			number:     "0xb",
		},
		{
			name:       "missing parent",
			blockHash:  "0x0b",
			headerHash: "0x0b",
			parentHash: "0xmissing",
			number:     "0xb",
		},
		{
			name:       "non consecutive height",
			blockHash:  "0x0b",
			headerHash: "0x0b",
			parentHash: "0x0a",
			number:     "0xc",
		},
		{
			name:       "missing header",
			blockHash:  "0x0b",
			headerHash: "",
			parentHash: "",
			number:     "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := env.taskPool.Stats().EnqueuedHeaderHash
			if env.flow.EnqueueRemoteHeaderCandidate(tc.blockHash, tc.headerHash, tc.parentHash, tc.number) {
				t.Fatal("expected invalid remote header candidate to be rejected")
			}
			after := env.taskPool.Stats().EnqueuedHeaderHash
			if after != before {
				t.Fatalf("expected no header-by-hash task, before=%d after=%d", before, after)
			}
		})
	}
}
