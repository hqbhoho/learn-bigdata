package com.hqbhoho.bigdata.hive.parse1.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/05/22
 */
public class FileUtils {
    /**
     * 层序遍历文件
     *
     * @param root
     * @return
     */
    public static List<String> traversalFileDirectory(File root) {
        ArrayList<String> res = new ArrayList<>();
        LinkedList<File> queue = new LinkedList<>();
        queue.add(root);
        while (!queue.isEmpty()) {
            File file = queue.removeLast();
            if (file.isFile()) {
                res.add(file.getAbsolutePath());
            }
            if (file.isDirectory()) {
                File[] files = file.listFiles();
                Stream.of(files).forEach(f -> queue.addFirst(f));
            }
        }
        return res;
    }
}
