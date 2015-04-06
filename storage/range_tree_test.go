// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Bram Gruneir (bram.gruneir@gmail.com)

package storage

import (
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func createTreeContext(rootKey *proto.Key, nodes []*proto.RangeTreeNode) *treeContext {
	root := &proto.RangeTree{
		RootKey: *rootKey,
	}
	tc := &treeContext{
		db:    nil,
		tree:  root,
		dirty: false,
		nodes: map[string]cachedNode{},
	}
	for _, node := range nodes {
		// We don't use setNode here to ensure updated is false.
		tc.nodes[string(node.Key)] = cachedNode{
			node:  node,
			dirty: false,
		}
	}
	return tc
}

// TestIsRed ensures that the isRed function is correct.
func TestIsRed(t *testing.T) {
	defer leaktest.AfterTest(t)
	testCases := []struct {
		node     *proto.RangeTreeNode
		expected bool
	}{
		// normal black node
		{&proto.RangeTreeNode{Black: true}, false},
		// normal red node
		{&proto.RangeTreeNode{Black: false}, true},
		// nil
		{nil, false},
	}
	for i, test := range testCases {
		node := test.node
		actual := isRed(node)
		if actual != test.expected {
			t.Errorf("%d: %+v expect %v; got %v", i, node, test.expected, actual)
		}
	}
}

// TestFlip ensures that flips function correctly.
func TestFlip(t *testing.T) {
	defer leaktest.AfterTest(t)
	testCases := []struct {
		nodeBlack          bool
		leftBlack          bool
		rightBlack         bool
		expectedNodeBlack  bool
		expectedLeftBlack  bool
		expectedRightBlack bool
	}{
		{true, true, true, false, false, false},
		{false, false, false, true, true, true},
		{true, false, true, false, true, false},
		{false, true, false, true, false, true},
	}
	keyNode := proto.Key("Node")
	keyLeft := proto.Key("Left")
	keyRight := proto.Key("Right")
	for i, test := range testCases {
		node := &proto.RangeTreeNode{
			Key:       keyNode,
			ParentKey: engine.KeyMin,
			Black:     test.nodeBlack,
			LeftKey:   &keyLeft,
			RightKey:  &keyRight,
		}
		left := &proto.RangeTreeNode{
			Key:       keyLeft,
			ParentKey: keyNode,
			Black:     test.leftBlack,
		}
		right := &proto.RangeTreeNode{
			Key:       keyRight,
			ParentKey: keyNode,
			Black:     test.rightBlack,
		}
		tc := createTreeContext(&keyNode, []*proto.RangeTreeNode{
			node,
			left,
			right,
		})
		_, err := tc.flip(node)

		// Are all three nodes dirty?
		if !tc.nodes[string(keyNode)].dirty {
			t.Errorf("%d: Expected node to be dirty", i)
		}
		if !tc.nodes[string(keyLeft)].dirty {
			t.Errorf("%d: Expected left node to be dirty", i)
		}
		if !tc.nodes[string(keyRight)].dirty {
			t.Errorf("%d: Expected right node to be dirty", i)
		}

		// Are the resulting nodes correctly flipped?
		actualNode, err := tc.getNode(&keyNode)
		if err != nil {
			t.Fatal(err)
		}
		if actualNode.Black != test.expectedNodeBlack {
			t.Errorf("%d: Expect node black to be %v, got %v", i, test.expectedNodeBlack, actualNode.Black)
		}
		actualLeft, err := tc.getNode(&keyLeft)
		if err != nil {
			t.Fatal(err)
		}
		if actualLeft.Black != test.expectedLeftBlack {
			t.Errorf("%d: Expect left node black to be %v, got %v", i, test.expectedLeftBlack, actualLeft.Black)
		}
		actualRight, err := tc.getNode(&keyRight)
		if err != nil {
			t.Fatal(err)
		}
		if actualRight.Black != test.expectedRightBlack {
			t.Errorf("%d: Expect right node black to be %v, got %v", i, test.expectedRightBlack, actualRight.Black)
		}
	}
}

/*
// TestRotateLeft ensures that left rotations occur corectly.
func TestRotateLeft(t *testing.T) {
	defer leaktest.AfterTest(T)
	testCases := []struct {
		treeContext  *treeContext
		node         *proto.RangeTreeNode
		expectedNode *proto.RangeTreeNode
		expectedLeft *proto.RangeTreeNode

	}
}
*/
