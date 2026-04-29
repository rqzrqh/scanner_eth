package headernotify

import "testing"

func TestRemoteChainUpdateMonotonicHeight(t *testing.T) {
	c := NewRemoteChain()
	c.Update(10, "a")
	c.Update(5, "b")
	h, hash := c.GetChainInfo()
	if h != 10 || hash != "a" {
		t.Fatalf("stale lower height must be ignored: got height=%d hash=%s want 10,a", h, hash)
	}

	c.Update(10, "c")
	h, hash = c.GetChainInfo()
	if h != 10 || hash != "c" {
		t.Fatalf("same-height update must refresh hash: got height=%d hash=%s want 10,c", h, hash)
	}

	c.Update(11, "d")
	h, hash = c.GetChainInfo()
	if h != 11 || hash != "d" {
		t.Fatalf("advance: got height=%d hash=%s want 11,d", h, hash)
	}
}
