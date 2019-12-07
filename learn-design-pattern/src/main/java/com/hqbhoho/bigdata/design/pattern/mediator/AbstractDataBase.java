package com.hqbhoho.bigdata.design.pattern.mediator;

import java.util.ArrayList;
import java.util.List;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/07
 */
public abstract class AbstractDataBase implements DataBase {
    private DataBaseMediator dataBaseMediator;
    private List<String> dataList;


    public AbstractDataBase() {
        this.dataList = new ArrayList<>();
    }

    @Override
    public void add(String data) {
        this.dataList.add(data);
    }

    @Override
    public void addData(String data) {
        add(data);
        syncData(data);
    }

    @Override
    public List<String> selectAll() {
        return this.dataList;
    }

    public DataBaseMediator getDataBaseMediator() {
        return dataBaseMediator;
    }

    public void setDataBaseMediator(DataBaseMediator dataBaseMediator) {
        this.dataBaseMediator = dataBaseMediator;
    }
}
