package com.hqbhoho.bigdata.design.pattern.composite;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/04
 */
public class TestDemo {
    public static void main(String[] args) {
        File docu3 = new Document("docu-3");
        Folder f2 = new Folder("folder-2");
        f2.addFile(docu3);
        File docu1 = new Document("docu-1");
        File docu2 = new Document("docu-2");
        Folder f1 = new Folder("folder-1");
        f1.addFile(docu1);
        f1.addFile(docu2);
        f2.addFile(f1);
        f2.show();
        System.out.println("==============del docu-2================");
        f1.removeFile(docu2);
        f2.show();
    }
}
