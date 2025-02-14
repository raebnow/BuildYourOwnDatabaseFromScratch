package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unsafe"
)

const HEADER = 4

const (
	BNODE_NODE = 1 // internal nodes without values
	BNODE_LEAF = 2 // leaf nodes with values
)

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

func assert(b bool) {
	if b == false {
		panic("assertion failed")
	}
}

func init() {
	node1max := 4 + 1*8 + 1*2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	assert(node1max <= BTREE_PAGE_SIZE)
}

type BNode []byte // can be dumped to the disk

// getters
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

// setter
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

// read and write the child pointers array
func (node BNode) getPtr(idx uint16) uint64 {
	assert(idx < node.nkeys())
	pos := 4 + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	assert(idx < node.nkeys())
	pos := 4 + 8*idx
	binary.LittleEndian.PutUint64(node[pos:], val)
}

func offsetPos(node BNode, idx uint16) uint16 {
	assert(1 <= idx && idx <= node.nkeys())
	return 4 + 8*node.nkeys() + 2*(idx-1)
}

// read the `offsets` array
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}

	pos := 4 + 8*node.nkeys() + 2*(idx-1)
	return binary.LittleEndian.Uint16(node[pos:])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node[offsetPos(node, idx):], offset)
}

func (node BNode) kvPos(idx uint16) uint16 {
	assert(idx <= node.nkeys())
	return 4 + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos+0:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+klen:][:vlen]
}

func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	new.setPtr(idx, ptr)

	// KVs
	pos := new.kvPos(idx) // uses the offset value of the previous key

	// 4-bytes KV sizes
	binary.LittleEndian.PutUint16(new[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))

	// KV data
	copy(new[pos+4:], key)
	copy(new[pos+4+uint16(len(key)):], val)

	// update the offset value for the next key
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16((len(key)+len(val))))
}

func (node BNode) nbytes() uint16 {
	// uses the offset value of the last day
	return node.kvPos(node.nkeys())
}

func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)                   // copy the keys before 'idx'
	nodeAppendKV(new, idx, 0, key, val)                    // the new key
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx) // keys from 'idx'
}

// copy multiple keys, values, and pointers into the position
func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	for i := uint16(0); i < n; i++ {
		dst, src := dstNew+i, srcOld+i
		nodeAppendKV(new, dst, old.getPtr(src), old.getKey(src), old.getVal(src))
	}
}

func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-(idx+1))
}

// find the last position that is less than or equal to the key
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()

	var i uint16
	for i = 0; i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp == 0 {
			return i
		}

		if cmp > 0 {
			return i - 1
		}
	}

	return i - 1
}

// Split an oversized node into 2 nodes. The 2nd node always fits.
func nodeSplit2(left BNode, right BNode, old BNode) {
	assert(old.nkeys() >= 2)

	// the initial guess
	nleft := old.nkeys() / 2

	// try to fit the left half
	left_bytes := func() uint16 {
		return 4 + 8*nleft + 2*nleft + old.getOffset(nleft)
	}

	for left_bytes() > BTREE_PAGE_SIZE {
		nleft--
	}

	assert(nleft >= 1)

	// try to fit the right half
	right_bytes := func() uint16 {
		return old.nbytes() - left_bytes() + 4
	}

	for right_bytes() > BTREE_PAGE_SIZE {
		nleft++
	}

	assert(nleft < old.nkeys())

	nright := old.nkeys() - nleft

	// new nodes
	left.setHeader(old.btype(), nleft)
	right.setHeader(old.btype(), nright)
	nodeAppendRange(left, old, 0, 0, nleft)
	nodeAppendRange(right, old, 0, nleft, nright)

	// NOTE: the left half may be still too big
	assert(right.nbytes() <= BTREE_PAGE_SIZE)
}

// split a node if it's too big. the results are 1-3 nodes.
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old} // not split
	}

	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE)) // might be split later
	right := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(left, right, old)

	if left.nbytes() <= BTREE_PAGE_SIZE {
		left = left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right} // split into 2 nodes// 2 nodes
	}

	leftleft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(leftleft, middle, left)

	assert(leftleft.nbytes() <= BTREE_PAGE_SIZE)
	return 3, [3]BNode{leftleft, middle, right} // 3 nodes
}

type BTree struct {
	// root pointer (a nonzero page number)
	root uint64

	// callbacks for managing on-disk pages
	get func(uint64) []byte // read data from a page number
	new func([]byte) uint64 // allocate a new page number with data
	del func(uint64)        // deallocate a page number
}

func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// The extra size allows it to exceed 1 page temporarily
	new := BNode(make([]byte, 2*BTREE_PAGE_SIZE))

	// where to insert the key
	idx := nodeLookupLE(node, key) // node.getKey(idx) <= key

	switch node.btype() {
	case BNODE_LEAF: // leaf node
		if bytes.Equal(key, node.getKey(idx)) {
			leafUpdate(new, node, idx, key, val) // found, update it
		} else {
			leafInsert(new, node, idx+1, key, val) // not found, insert
		}

	case BNODE_NODE: // internal node, walk into the child node
		// recursive insertion to the kid node
		kptr := node.getPtr(idx)
		knode := treeInsert(tree, tree.get(kptr), key, val)

		// after insertion, split the result
		nsplit, split := nodeSplit3(knode)

		// deallocate the old kid node
		tree.del(kptr)

		// update the kid links
		nodeReplaceKidN(tree, new, node, idx, split[:nsplit]...)
	}

	return new
}

// replace a link with multiple links
func nodeReplaceKidN(tree *BTree, new BNode, old BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))

	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)

	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}

	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

func (tree *BTree) Insert(key []byte, val []byte) {
	if tree.root == 0 {
		// create the first node
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_LEAF, 2)

		// a dummy key, this makes the tree cover the whole key space.
		// thus a lookup can always find a containing node.
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return
	}

	node := treeInsert(tree, tree.get(tree.root), key, val)
	nsplit, split := nodeSplit3(node)
	tree.del(tree.root)
	if nsplit > 1 {
		// the root was split, add a new level.
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_NODE, nsplit)

		for i, knode := range split[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}

		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}
}

func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}

	if idx > 0 {
		sibling := BNode(tree.get(node.getPtr(idx - 1)))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling // left
		}
	}

	if idx+1 < node.nkeys() {
		sibling := BNode(tree.get(node.getPtr(idx + 1)))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling // right
		}
	}

	return 0, BNode{}
}

// Implement missing nodeMerge function(by ChatGPT)
func nodeMerge(dest BNode, left BNode, right BNode) {
	dest.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(dest, left, 0, 0, left.nkeys())
	nodeAppendRange(dest, right, left.nkeys(), 0, right.nkeys())
}

// Implement missing nodeReplace2Kid function(by ChatGPT)
func nodeReplace2Kid(new BNode, old BNode, idx uint16, ptr uint64, key []byte) {
	new.setHeader(BNODE_NODE, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, ptr, key, nil)
	nodeAppendRange(new, old, idx+1, idx+2, old.nkeys()-(idx+1))
}

// by ChatGPT
func treeSearch(tree *BTree, node BNode, key []byte) ([]byte, bool) {
	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		if idx < node.nkeys() && bytes.Equal(node.getKey(idx), key) {
			return node.getVal(idx), true
		}
		return nil, false

	case BNODE_NODE:
		return treeSearch(tree, tree.get(node.getPtr(idx)), key)
	}

	return nil, false
}

// by ChatGPT
func (tree *BTree) Search(key []byte) ([]byte, bool) {
	if tree.root == 0 {
		return nil, false
	}
	return treeSearch(tree, tree.get(tree.root), key)
}

// by ChatGPT
func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		if idx < node.nkeys() && bytes.Equal(node.getKey(idx), key) {
			new := BNode(make([]byte, BTREE_PAGE_SIZE))
			new.setHeader(BNODE_LEAF, node.nkeys()-1)
			nodeAppendRange(new, node, 0, 0, idx)
			nodeAppendRange(new, node, idx, idx+1, node.nkeys()-idx-1)
			return new
		}
		return BNode{}

	case BNODE_NODE:
		updated := nodeDelete(tree, node, idx, key)
		if len(updated) == 0 {
			return BNode{}
		}
		return updated
	}

	return BNode{}
}

// by ChatGPT
func (tree *BTree) Delete(key []byte) {
	if tree.root == 0 {
		return
	}
	node := treeDelete(tree, tree.get(tree.root), key)
	if len(node) > 0 {
		tree.root = tree.new(node)
	}
}

// by ChatGPT
func treeTraverse(tree *BTree, node BNode, visit func(key, val []byte)) {
	switch node.btype() {
	case BNODE_LEAF:
		for i := uint16(1); i < node.nkeys(); i++ {
			visit(node.getKey(i), node.getVal(i))
		}
	case BNODE_NODE:
		for i := uint16(0); i < node.nkeys(); i++ {
			treeTraverse(tree, tree.get(node.getPtr(i)), visit)
		}
	}
}

// by ChatGPT
func (tree *BTree) Traverse(visit func(key, val []byte)) {
	if tree.root == 0 {
		return
	}
	treeTraverse(tree, tree.get(tree.root), visit)
}

// delete a key from an internal node; part of the treeDelete()
func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	// recurse into the kid
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(kptr), key)
	if len(updated) == 0 {
		return BNode{} // not found
	}
	tree.del(kptr)

	new := BNode(make([]byte, BTREE_PAGE_SIZE))
	// check for merging
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: // left
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplace2Kid(new, node, idx-1, tree.new(merged), merged.getKey(0))

	case mergeDir > 0: // right
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Kid(new, node, idx, tree.new(merged), merged.getKey(0))

	case mergeDir == 0 && updated.nkeys() == 0:
		assert(node.nkeys() == 1 && idx == 0) // 1 empty child but no sibling
		new.setHeader(BNODE_NODE, 0)          // the parent becomes empty too

	case mergeDir == 0 && updated.nkeys() > 0: // no merge
		nodeReplaceKidN(tree, new, node, idx, updated)
	}

	return new
}

type C struct {
	tree  BTree
	ref   map[string]string // the reference data
	pages map[uint64]BNode  // in-memory pages
}

func newC() *C {
	pages := map[uint64]BNode{}

	return &C{
		tree: BTree{
			get: func(ptr uint64) []byte {
				node, ok := pages[ptr]
				assert(ok)
				return node
			},
			new: func(node []byte) uint64 {
				assert(BNode(node).nbytes() <= BTREE_PAGE_SIZE)
				ptr := uint64(uintptr(unsafe.Pointer(&node[0])))
				assert(pages[ptr] == nil)
				pages[ptr] = node
				return ptr
			},
			del: func(ptr uint64) {
				assert(pages[ptr] != nil)
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *C) add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val // reference data
}

func main() {
	// Initialize the B+ Tree
	btree := newC()

	// Insert some key-value pairs
	btree.add("apple", "red")
	btree.add("banana", "yellow")
	btree.add("grape", "purple")
	btree.add("orange", "orange")
	btree.add("cherry", "red")

	// Traverse the tree and print key-value pairs
	fmt.Println("B+ Tree Contents:")
	btree.tree.Traverse(func(key, val []byte) {
		fmt.Printf("%s -> %s\n", string(key), string(val))
	})

	// Test searching for keys
	searchKeys := []string{"apple", "banana", "mango"}
	fmt.Println("\nSearch Results:")
	for _, key := range searchKeys {
		if value, found := btree.tree.Search([]byte(key)); found {
			fmt.Printf("Found: %s -> %s\n", key, string(value))
		} else {
			fmt.Printf("Not Found: %s\n", key)
		}
	}
}
