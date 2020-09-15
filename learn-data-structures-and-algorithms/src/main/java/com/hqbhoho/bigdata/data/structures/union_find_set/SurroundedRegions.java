package com.hqbhoho.bigdata.data.structures.union_find_set;

public class SurroundedRegions {


    /**
     * 我们的思路是把所有边界上的 OO 看做一个连通区域。遇到 OO 就执行并查集合并操作，这样所有的 OO 就会被分成两类
     * <p>
     * 和边界上的 OO 在一个连通区域内的。这些 OO 我们保留。
     * 不和边界上的 OO 在一个连通区域内的。这些 OO 就是被包围的，替换。
     */
    public static void main(String[] args) {

    }

    public static void calc(char[][] board) {
        if (board == null || board.length == 0 || board[0].length == 0) {
            return;
        }

        int rowNum = board.length;
        int colNum = board[0].length;
        FriendCircles.UnionFindSet unionFindSet = new FriendCircles.UnionFindSet(rowNum * colNum +1);

        int specialNode = rowNum * colNum +1;

        for (int i = 0; i < rowNum; i++) {
            for (int j = 0; j < colNum; j++) {

                if(board[i][j] == 'X'){
                    continue;
                }

                // 边缘的节点 O 全部并入最后一个parent  排除出所有与边缘节点关联的 'O'  节点
                if(board[i][j] == 'O' &&(i == 0 || i == rowNum -1 || j == 0 || j == colNum -1)){
                    int node = i* rowNum + j;
                    unionFindSet.union(node ,specialNode);
                }


            }

        }


    }
}
