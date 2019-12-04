package com.hqbhoho.bigdata.design.pattern.composite;

import java.util.ArrayList;
import java.util.List;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/04
 */
public class Folder implements File {
    private String name;
    private List<File> files;

    public Folder(String name) {
        this.name = name;
        files = new ArrayList<>();
    }

    public void addFile(File file) {
        files.add(file);
    }

    public void removeFile(File file) {
        files.remove(file);
    }

    @Override
    public void show() {
        System.out.println("-->>"+name);
        files.forEach(f->f.show());
    }
}
