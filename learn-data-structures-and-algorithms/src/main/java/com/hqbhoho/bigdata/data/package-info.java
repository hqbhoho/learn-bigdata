package com.hqbhoho.bigdata.data;
/**
 * describe:
 * <p>
 * Structures And Algorithms
 *
 * **************************************************************
 * (1) Array
 *  https://leetcode-cn.com/problems/container-with-most-water/
 *  {@link com.hqbhoho.bigdata.data.structures.array.ContainerWithMostWater}
 *	https://leetcode-cn.com/problems/move-zeroes/
 *  {@link com.hqbhoho.bigdata.data.structures.array.MoveZeroes}
 *	https://leetcode.com/problems/climbing-stairs/
 *  {@link com.hqbhoho.bigdata.data.structures.array.ClimbingStairs}
 *	https://leetcode-cn.com/problems/3sum/ 
 *  {@link com.hqbhoho.bigdata.data.structures.array.ThreeSum}
 *  https://leetcode-cn.com/problems/remove-duplicates-from-sorted-array/
 *  {@link com.hqbhoho.bigdata.data.structures.array.RemoveDupliFromSortedArray}
 *  https://leetcode-cn.com/problems/rotate-array/
 *  {@link com.hqbhoho.bigdata.data.structures.array.RotateArray}
 *  https://leetcode-cn.com/problems/merge-sorted-array/
 *  {@link com.hqbhoho.bigdata.data.structures.array.MergeSortedArray}
 *  https://leetcode-cn.com/problems/two-sum/
 *  {@link com.hqbhoho.bigdata.data.structures.array.TwoSum}
 *  https://leetcode-cn.com/problems/plus-one/
 *  {@link com.hqbhoho.bigdata.data.structures.array.PlusOne}
 *  https://leetcode.com/problems/trapping-rain-water/
 *  {@link com.hqbhoho.bigdata.data.structures.array.TrappingRainWater}
 *
 * **************************************************************
 * (2) LinkList
 *  https://leetcode.com/problems/reverse-linked-list/
 *  {@link com.hqbhoho.bigdata.data.structures.linklist.ReverseLinkedList}
 *	https://leetcode.com/problems/swap-nodes-in-pairs
 *  {@link com.hqbhoho.bigdata.data.structures.linklist.SwapNodesInPairs}
 *	https://leetcode.com/problems/linked-list-cycle
 *  {@link com.hqbhoho.bigdata.data.structures.linklist.LinkedListCycle}
 *	https://leetcode.com/problems/linked-list-cycle-ii
 *  {@link com.hqbhoho.bigdata.data.structures.linklist.LinkedListCycleII}
 *  https://leetcode.com/problems/reverse-nodes-in-k-group/
 *  {@link com.hqbhoho.bigdata.data.structures.linklist.ReverseNodesInKGroup}
 *  https://leetcode-cn.com/problems/merge-two-sorted-lists/
 *  {@link com.hqbhoho.bigdata.data.structures.linklist.MergeTwoSortedList}
 *
 * **************************************************************
 * (3) Stack
 *  https://leetcode-cn.com/problems/valid-parentheses/
 *  {@link com.hqbhoho.bigdata.data.structures.stack.VaildParentheses}
 *  https://leetcode-cn.com/problems/min-stack/
 *  {@link com.hqbhoho.bigdata.data.structures.stack.MinStack}
 *  https://leetcode-cn.com/problems/largest-rectangle-in-histogram
 *  {@link com.hqbhoho.bigdata.data.structures.stack.largestRectangleInHistogram}
 *
 * **************************************************************
 * (4) queue
 *  https://leetcode-cn.com/problems/sliding-window-maximum/
 *  {@link com.hqbhoho.bigdata.data.structures.queue.SlidingWindowMaximum}
 *  https://leetcode.com/problems/design-circular-deque
 *  {@link com.hqbhoho.bigdata.data.structures.queue.CircularDeque}
 *
 * **************************************************************
 * (5) hashtable
 *  https://leetcode-cn.com/problems/valid-anagram/description/
 *  {@link com.hqbhoho.bigdata.data.structures.hashtable.ValidAnagram}
 *  https://leetcode-cn.com/problems/group-anagrams/
 *  {@link com.hqbhoho.bigdata.data.structures.hashtable.GroupAnagrams}
 *
 * **************************************************************
 * (6) tree
 *  https://leetcode-cn.com/problems/binary-tree-inorder-traversal/
 *  {@link com.hqbhoho.bigdata.data.structures.tree.binarytree.BinaryTreeTraversal#midOrder(com.hqbhoho.bigdata.data.structures.tree.binarytree.BinaryTreeTraversal.TreeNode, java.util.List)}
 *	https://leetcode-cn.com/problems/binary-tree-preorder-traversal/
 *  {@link com.hqbhoho.bigdata.data.structures.tree.binarytree.BinaryTreeTraversal#preOrder(com.hqbhoho.bigdata.data.structures.tree.binarytree.BinaryTreeTraversal.TreeNode, java.util.List)}
 *	https://leetcode-cn.com/problems/n-ary-tree-postorder-traversal/
 *  {@link com.hqbhoho.bigdata.data.structures.tree.narytree.NaryTreeTraversal#postOrder(com.hqbhoho.bigdata.data.structures.tree.narytree.NaryTreeTraversal.Node, java.util.List)}
 *	https://leetcode-cn.com/problems/n-ary-tree-preorder-traversal/
 *  {@link com.hqbhoho.bigdata.data.structures.tree.narytree.NaryTreeTraversal#preOrder(com.hqbhoho.bigdata.data.structures.tree.narytree.NaryTreeTraversal.Node, java.util.List)}
 *	https://leetcode-cn.com/problems/n-ary-tree-level-order-traversal/
 * 	{@link com.hqbhoho.bigdata.data.structures.tree.narytree.NaryTreeTraversal#levelOrder(com.hqbhoho.bigdata.data.structures.tree.narytree.NaryTreeTraversal.Node, java.util.List)}
 *
 *
 *
 * **************************************************************
 *  Algorithms
 * **************************************************************
 * (1) recusion
 *	https://leetcode-cn.com/problems/generate-parentheses/
 * 	{@link com.hqbhoho.bigdata.data.algorithms.recursion.GenerateParenthesis}
 * 	https://leetcode-cn.com/problems/invert-binary-tree/description/
 * 	{@link com.hqbhoho.bigdata.data.algorithms.recursion.InvertBinaryTree}
 * 	https://leetcode-cn.com/problems/validate-binary-search-tree
 * 	{@link com.hqbhoho.bigdata.data.algorithms.recursion.ValidateBinarySearchTree}
 *  https://leetcode-cn.com/problems/maximum-depth-of-binary-tree
 *  {@link com.hqbhoho.bigdata.data.algorithms.recursion.MaxiumDepthOfBinaryTree}
 *  https://leetcode-cn.com/problems/minimum-depth-of-binary-tree
 *  {@link com.hqbhoho.bigdata.data.algorithms.recursion.MinimumDepthOfBinaryTree}
 *  https://leetcode-cn.com/problems/serialize-and-deserialize-binary-tree/
 *  {@link com.hqbhoho.bigdata.data.algorithms.recursion.CodecBinaryTree}
 *  https://leetcode-cn.com/problems/lowest-common-ancestor-of-a-binary-tree/
 *  {@link com.hqbhoho.bigdata.data.algorithms.recursion.LowestCommonAncestorOfTree}
 *  https://leetcode-cn.com/problems/construct-binary-tree-from-preorder-and-inorder-traversal
 *
 *
 *
 *
 *
 *
 *
 *
 *	https://leetcode-cn.com/problems/combinations/
 *	https://leetcode-cn.com/problems/permutations/
 *	https://leetcode-cn.com/problems/permutations-ii/
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/10
 */