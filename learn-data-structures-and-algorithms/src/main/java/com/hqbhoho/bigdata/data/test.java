package com.hqbhoho.bigdata.data;

import javafx.util.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/06/08
 */
public class test {
    public static void main(String[] args) {

    }

    public static List<List<String>> getRes(String word){
        List<List<String>> res = new ArrayList<>();
        if(word == null && word.length() == 0 ) return null;
        char[] chars = word.toCharArray();
        for(int i = 0;i< chars.length;){
            for(int j = i+1 ;j<chars.length;) {

            }
        }




    }

    public void sortFile(String inputFilePath, String outputFilePath) {
        int BATCH_SIZE = 10000;
        String LINE_SEPARATOR = "\r\n";
        List<String> fileNameList = new ArrayList<>();
        // 切分小文件
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFilePath)); BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
            int index = 0;
            List<Integer> batchLineList = new ArrayList<>(BATCH_SIZE);
            String line;
            while ((line = reader.readLine()) != null) {
                batchLineList.add(Integer.valueOf(line));
                if (batchLineList.size() == BATCH_SIZE) {
                    // 内容排序
                    batchLineList.sort(Comparator.comparingInt(a -> a));
                    // 写小文件
                    String fileName = inputFilePath + ".tmp." + index++;
                    try (FileWriter tmpWriter = new FileWriter(fileName)) {
                        for (Integer val : batchLineList) {
                            tmpWriter.write(val + LINE_SEPARATOR);
                        }
                    }
                    fileNameList.add(fileName);
                    batchLineList.clear();
                }
            }

            List<BufferedReader> readers = new ArrayList<>();
            for (String tempPath : fileNameList) {
                readers.add(new BufferedReader(new FileReader(tempPath)));
            }
            // 多路归并排序
            PriorityQueue<Pair<BufferedReader, Integer>> heap = new PriorityQueue<>((p1, p2) -> p1.getValue() - p2.getValue());

            for (BufferedReader fr : readers) {
                if ((line = fr.readLine()) != null) {
                    heap.add(new Pair<>(fr, Integer.valueOf(line)));
                }
            }

            while (!heap.isEmpty()) {
                Pair<BufferedReader, Integer> curr = heap.poll();
                writer.write(curr.getValue() + LINE_SEPARATOR);
                BufferedReader currReader = curr.getKey();
                if ((line = currReader.readLine()) != null) {
                    heap.add(new Pair<>(currReader, Integer.valueOf(line)));
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
