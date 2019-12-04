package com.hqbhoho.bigdata.design.pattern.composite;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/04
 */
public class Document implements File {

    private String name;

    public Document(String name) {
        this.name = name;
    }

    @Override
    public void show() {
        System.out.println("---->>>>"+name);
    }
}
