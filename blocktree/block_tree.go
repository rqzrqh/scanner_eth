package blocktree

import "sort"

// Key is the identifier type for tree nodes.
// Declared as a type alias so it can be swapped without touching other code.
type Key = string

// Node holds the core fields of a block.
type Node struct {
	Height    uint64
	Key       Key
	ParentKey Key
	Weight    uint64
	Header    interface{}
}

// IrreversibleNode represents the irreversible ancestor of a block.
type IrreversibleNode struct {
	Height uint64
	Key    Key
}

// LinkedNode extends Node with cached irreversibility information.
type LinkedNode struct {
	Node
	Data         interface{}
	Irreversible *IrreversibleNode
}

// Branch represents one chain branch from a header back to the linked root.
// Nodes are ordered by Height ascending.
type Branch struct {
	Header *LinkedNode
	Nodes  []*LinkedNode
}

// BlockTree is a fork-aware block tree. Blocks whose parent is not yet known
// are buffered as orphans and adopted once the parent arrives.
//
// Not safe for concurrent use without external synchronisation.
type BlockTree struct {
	// keyValue maps a block key to its LinkedNode.
	keyValue map[Key]*LinkedNode

	// parentToChild maps a parent key to the set of its direct children.
	parentToChild map[Key]map[Key]struct{}

	// headers is the set of leaf keys (nodes with no children yet).
	headers map[Key]struct{}

	// root is the oldest retained block. nil when the tree is empty.
	root *Node

	// orphanParentToChild stores blocks whose parent has not been seen yet.
	// Indexed as: parentKey -> childKey -> Node.
	orphanParentToChild map[Key]map[Key]*Node

	// orphanKeySet records all keys currently buffered in orphanParentToChild.
	orphanKeySet map[Key]struct{}

	// irreversibleCount is the number of confirmations before a block is
	// considered irreversible.
	irreversibleCount int
}

// NewBlockTree creates an empty BlockTree.
func NewBlockTree(irreversibleCount int) *BlockTree {
	return &BlockTree{
		keyValue:            make(map[Key]*LinkedNode),
		parentToChild:       make(map[Key]map[Key]struct{}),
		headers:             make(map[Key]struct{}),
		orphanParentToChild: make(map[Key]map[Key]*Node),
		orphanKeySet:        make(map[Key]struct{}),
		irreversibleCount:   irreversibleCount,
	}
}

// Insert adds a block to the tree. If the parent is unknown the block is
// buffered as an orphan and adopted automatically when its parent arrives.
//
// Returns the list of LinkedNodes inserted during this call (the block itself
// plus any cascadingly resolved orphans). Returns nil if the block already
// existed or was buffered as an orphan.
//
// header is metadata attached to Node.
func (t *BlockTree) Insert(height uint64, key, parentKey Key, weight uint64, header interface{}) []*LinkedNode {
	if _, exists := t.keyValue[key]; exists {
		return nil
	}
	if _, exists := t.orphanKeySet[key]; exists {
		return nil
	}

	_, parentExists := t.keyValue[parentKey]

	if parentExists || t.root == nil {
		if t.root == nil {
			t.root = &Node{Height: height, Key: key, ParentKey: parentKey, Weight: weight, Header: header}
		}
		return t.internalInsert(height, key, parentKey, weight, header)
	}

	// Parent unknown: buffer as orphan.
	if t.orphanParentToChild[parentKey] == nil {
		t.orphanParentToChild[parentKey] = make(map[Key]*Node)
	}
	t.orphanParentToChild[parentKey][key] = &Node{Height: height, Key: key, ParentKey: parentKey, Weight: weight, Header: header}
	t.orphanKeySet[key] = struct{}{}
	return nil
}

// internalInsert links the block into the tree and resolves orphans waiting
// on this block. Does NOT call prune.
func (t *BlockTree) internalInsert(height uint64, key, parentKey Key, weight uint64, header interface{}) []*LinkedNode {
	perfectIrr := t.computeIrreversible(parentKey)

	nv := &LinkedNode{
		Node:         Node{Height: height, Key: key, ParentKey: parentKey, Weight: weight, Header: header},
		Irreversible: perfectIrr,
	}
	t.keyValue[key] = nv

	if t.parentToChild[parentKey] == nil {
		t.parentToChild[parentKey] = make(map[Key]struct{})
	}
	t.parentToChild[parentKey][key] = struct{}{}

	// The parent now has a child, so it is no longer a leaf.
	delete(t.headers, parentKey)
	// The new block has no children yet; it is a leaf.
	t.headers[key] = struct{}{}

	result := []*LinkedNode{nv}
	result = append(result, t.checkOrphan(key)...)
	return result
}

// computeIrreversible returns the irreversible ancestor of a node whose
// direct parent has key parentKey.
//
// The new node counts as hop 0, so the walk starts at parentKey (hop 1) and
// continues for irreversibleCount-1 more hops.
//
// Returns nil when the chain is shallower than irreversibleCount or the node
// is not found.
func (t *BlockTree) computeIrreversible(parentKey Key) *IrreversibleNode {
	if t.irreversibleCount <= 0 || parentKey == "" {
		return nil
	}
	currentKey := parentKey
	for i := 1; i < t.irreversibleCount; i++ {
		nv, ok := t.keyValue[currentKey]
		if !ok || nv.ParentKey == "" {
			return nil
		}
		currentKey = nv.ParentKey
	}
	nv, ok := t.keyValue[currentKey]
	if !ok {
		return nil
	}
	return &IrreversibleNode{Height: nv.Height, Key: nv.Key}
}

// checkOrphan resolves orphans whose parent key matches key, inserting them
// via internalInsert and cascading until no more orphans can be resolved.
func (t *BlockTree) checkOrphan(key Key) []*LinkedNode {
	siblings, ok := t.orphanParentToChild[key]
	if !ok {
		return nil
	}
	delete(t.orphanParentToChild, key)

	var result []*LinkedNode
	for _, v := range siblings {
		delete(t.orphanKeySet, v.Key)
		result = append(result, t.internalInsert(v.Height, v.Key, v.ParentKey, v.Weight, v.Header)...)
	}
	return result
}

// Prune removes blocks that have become irreversibly buried under the longest
// chain.
func (t *BlockTree) Prune() { t.prune() }

func (t *BlockTree) prune() {
	if t.root == nil || len(t.headers) == 0 {
		return
	}

	// Find the best leaf: greatest height, ties broken by heaviest weight.
	var bestNV *LinkedNode
	for k := range t.headers {
		nv := t.keyValue[k]
		if nv == nil {
			continue
		}
		if bestNV == nil ||
			nv.Height > bestNV.Height ||
			(nv.Height == bestNV.Height && nv.Weight > bestNV.Weight) {
			bestNV = nv
		}
	}
	if bestNV == nil || bestNV.Irreversible == nil || bestNV.Irreversible.Key == "" {
		return
	}

	newRootNV, ok := t.keyValue[bestNV.Irreversible.Key]
	if !ok {
		return
	}

	// Only prune when the new root is strictly deeper than the current root.
	if newRootNV.Height <= t.root.Height {
		return
	}

	// Walk backwards from the new root towards the old root.
	// At each step, delete sibling branches, then delete the parent node.
	current := newRootNV
	for {
		parentKey := current.ParentKey
		if parentKey == "" {
			break
		}
		parentNV, ok := t.keyValue[parentKey]
		if !ok {
			break
		}

		// Delete all children of parentKey except the one on the main path.
		for childKey := range t.parentToChild[parentKey] {
			if childKey != current.Key {
				walk(t, childKey)
			}
		}

		delete(t.keyValue, parentKey)
		delete(t.parentToChild, parentKey)

		if parentKey == t.root.Key {
			break
		}
		current = parentNV
	}

	rootValue := newRootNV.Node
	t.root = &rootValue
}

// walk recursively removes the entire subtree rooted at key from the tree.
func walk(t *BlockTree, key Key) {
	for childKey := range t.parentToChild[key] {
		walk(t, childKey)
	}
	// Non-leaf nodes are not in headers; this delete is a safe no-op for them.
	delete(t.headers, key)
	delete(t.keyValue, key)
	delete(t.parentToChild, key)
}

// Root returns a copy of the current root, or nil if the tree is empty.
func (t *BlockTree) Root() *Node {
	if t.root == nil {
		return nil
	}
	v := *t.root
	return &v
}

// Get returns the LinkedNode for key, or nil if not present.
func (t *BlockTree) Get(key Key) *LinkedNode {
	return t.keyValue[key]
}

// SetData attaches data to a linked node.
//
// data is typically a pointer payload; callers can check for nil to determine
// whether data has been attached.
//
// Returns false when key is not present in the linked tree.
func (t *BlockTree) SetData(key Key, data interface{}) bool {
	node := t.keyValue[key]
	if node == nil {
		return false
	}
	node.Data = data
	return true
}

// HeightRange returns the current linked tree height range.
// start is the root height, end is the maximum height among headers.
// ok is false when the tree is empty.
func (t *BlockTree) HeightRange() (start, end uint64, ok bool) {
	if t.root == nil || len(t.keyValue) == 0 {
		return 0, 0, false
	}

	start = t.root.Height
	end = start
	for k := range t.headers {
		nv := t.keyValue[k]
		if nv != nil && nv.Height > end {
			end = nv.Height
		}
	}
	return start, end, true
}

// UnlinkedNodes returns each orphan bucket's parent key: the key that orphan
// blocks are waiting for. Only parents that are not themselves buffered as
// orphans (not in orphanKeySet) are included.
func (t *BlockTree) UnlinkedNodes() []Key {
	var result []Key
	for parentKey, children := range t.orphanParentToChild {
		if len(children) == 0 {
			continue
		}
		if _, ok := t.orphanKeySet[parentKey]; ok {
			continue
		}
		result = append(result, parentKey)
	}
	return result
}

// LinkedNodes returns all linked nodes currently present in keyValue.
func (t *BlockTree) LinkedNodes() []*LinkedNode {
	result := make([]*LinkedNode, 0, len(t.keyValue))
	for _, nv := range t.keyValue {
		if nv != nil {
			result = append(result, nv)
		}
	}
	return result
}

// Branches returns all branches by walking backward from every header.
//
// Branch ordering: header Height descending, then header Weight descending,
// then header Key ascending for deterministic output.
//
// Node ordering within a branch: header -> root.
func (t *BlockTree) Branches() []Branch {
	if len(t.headers) == 0 {
		return nil
	}

	headerList := make([]*LinkedNode, 0, len(t.headers))
	for k := range t.headers {
		nv := t.keyValue[k]
		if nv != nil {
			headerList = append(headerList, nv)
		}
	}

	sort.Slice(headerList, func(i, j int) bool {
		if headerList[i].Height != headerList[j].Height {
			return headerList[i].Height > headerList[j].Height
		}
		if headerList[i].Weight != headerList[j].Weight {
			return headerList[i].Weight > headerList[j].Weight
		}
		return headerList[i].Key < headerList[j].Key
	})

	result := make([]Branch, 0, len(headerList))
	for _, header := range headerList {
		nodes := make([]*LinkedNode, 0)
		for cur := header; cur != nil; {
			nodes = append(nodes, cur)
			parent, ok := t.keyValue[cur.ParentKey]
			if !ok {
				break
			}
			cur = parent
		}

		result = append(result, Branch{Header: header, Nodes: nodes})
	}

	return result
}

// Len returns the number of blocks in the tree (excluding orphans).
func (t *BlockTree) Len() int {
	return len(t.keyValue)
}
