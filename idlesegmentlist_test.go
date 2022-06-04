// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/neetsdkasu/avltree"
	"github.com/neetsdkasu/avltree/intkey"
)

func TestIdleSegmentListTree(t *testing.T) {
	tempfile, err := os.Create(filepath.Join(t.TempDir(), "test.unkodb"))
	if err != nil {
		t.Fatal(err)
	}
	defer tempfile.Close()

	file, err := InitializeFile(tempfile)
	if err != nil {
		t.Fatal(err)
	}

	tree := NewIdleSegmentListTree(file)

	lengthList := []int{888, 111, 555, 333, 444, 777, 666, 222, 999}

	for _, segmentLength := range lengthList {
		seg, err := file.CreateSegment(segmentLength)
		if err != nil {
			t.Fatal(err)
		}
		key := intkey.IntKey(seg.BufferSize())
		_, ok := avltree.Insert(tree, false, key, seg)
		if !ok {
			t.Fatalf("Broken tree  (%#v) (%#v)", seg, tree)
		}
	}

	order := make([]int, 0, len(lengthList))

	avltree.Iterate(tree, false, func(node avltree.Node) (_ bool) {
		seg := node.Value().(*Segment)
		order = append(order, seg.BufferSize())
		return
	})

	sort.Ints(lengthList)

	if !reflect.DeepEqual(order, lengthList) {
		t.Fatalf("Unmatch order (%#v)", order)
	}
}
