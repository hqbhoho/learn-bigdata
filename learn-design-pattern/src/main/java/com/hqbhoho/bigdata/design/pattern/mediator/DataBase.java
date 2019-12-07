package com.hqbhoho.bigdata.design.pattern.mediator;

import java.util.List;

public interface DataBase {
    void add(String data);
    void addData(String data);
    void syncData(String data);
    List<String> selectAll();
}
