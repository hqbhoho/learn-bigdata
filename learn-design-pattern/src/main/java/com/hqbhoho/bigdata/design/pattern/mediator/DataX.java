package com.hqbhoho.bigdata.design.pattern.mediator;

import java.util.HashMap;
import java.util.Map;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/07
 */
public class DataX implements DataBaseMediator {

    private Map<String,DataBase> dataBases;

    public DataX() {
        this.dataBases = new HashMap<>();
    }

    public void register(String name,DataBase dataBase){
        dataBases.put(name.toUpperCase(),dataBase);
        ((AbstractDataBase) dataBase).setDataBaseMediator(this);
    }

    @Override
    public void syncData(String name, String data) {
        if(name.equalsIgnoreCase("MYSQL")){
            // do nothing
        }
        if(name.equalsIgnoreCase("REDIS")){
            dataBases.get("MYSQL").add(data);
            dataBases.get("ES").add(data);
        }
        if(name.equalsIgnoreCase("ES")){
            dataBases.get("MYSQL").add(data);
        }
    }
}
