package headernotify

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"math/big"
)

func TestNewHeaderNotifier(t *testing.T) {
	hn := NewHeaderNotifier(3, nil)
	if hn == nil {
		t.Fatal("NewHeaderNotifier should return non-nil")
	}
	if hn.id != 3 {
		t.Fatalf("unexpected notifier id: %d", hn.id)
	}
}

func TestToRemoteHeader(t *testing.T) {
	if got := toRemoteHeader(nil); got != nil {
		t.Fatalf("expected nil remote header for nil input, got=%+v", got)
	}

	h := &ethTypes.Header{
		ParentHash: common.HexToHash("0x1234"),
		Number:     big.NewInt(16),
		Difficulty: big.NewInt(2),
	}
	rh := toRemoteHeader(h)
	if rh == nil {
		t.Fatal("expected non-nil remote header")
	}
	if rh.ParentHash != h.ParentHash.Hex() {
		t.Fatalf("parent hash mismatch: got=%s want=%s", rh.ParentHash, h.ParentHash.Hex())
	}
	if rh.Number != "0x10" {
		t.Fatalf("number mismatch: got=%s want=0x10", rh.Number)
	}
	if rh.Difficulty != "0x2" {
		t.Fatalf("difficulty mismatch: got=%s want=0x2", rh.Difficulty)
	}
	if rh.Hash == "" {
		t.Fatal("hash should not be empty")
	}

	h2 := &ethTypes.Header{ParentHash: common.HexToHash("0xabcd")}
	rh2 := toRemoteHeader(h2)
	if rh2.Number != "" || rh2.Difficulty != "" {
		t.Fatalf("expected empty number/difficulty for nil fields, got=%+v", rh2)
	}
}
