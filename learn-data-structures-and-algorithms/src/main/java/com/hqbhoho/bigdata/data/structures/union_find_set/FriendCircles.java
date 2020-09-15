package com.hqbhoho.bigdata.data.structures.union_find_set;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/06/08
 */
public class FriendCircles {
    public static void main(String[] args) {

        int[][] M = new int[][]{
                new int[]{1, 0, 0},
                new int[]{0, 1, 0},
                new int[]{0, 0, 1}
        };

        System.out.println(calc(M));


    }

    public static int calc(int[][] M) {
        if (M == null || M.length == 0 || M[0].length == 0) {
            return 0;
        }
        UnionFindSet unionFindSet = new UnionFindSet(M.length);
        for (int i = 0; i < M.length; i++) {
            for (int j = 0; j < i; j++) {
                if (M[i][j] == 1) {
                    unionFindSet.union(i, j);
                }
            }
        }

        return unionFindSet.count;
    }

    /**
     * 并查集
     */
    static class UnionFindSet {
        int count;
        int[] parent;

        // 初始化并查集
        public UnionFindSet(int count) {
            this.count = count;
            parent = new int[this.count];
            for (int i = 0; i < count; i++) {
                parent[i] = i;
            }
        }

        // 查询父节点  路径压缩
        public int find(int x) {
            int temp = x;
            while (temp != parent[temp]) {
                temp = parent[temp];
            }
            parent[x] = temp;
            return temp;
        }

        // 合并
        public void union(int x, int y) {
            int p1 = find(x);
            int p2 = find(y);

            if (p1 != p2) {
                parent[p1] = p2;
                count--;
            }
        }
    }
}
