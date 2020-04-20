package com.hqbhoho.bigdata.customRPC.io;

import java.util.HashMap;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/19
 */
public class TransportConfig {
    private HashMap configs = new HashMap();

    public void setConfig(String key,Object value){
        configs.put(key,value);
    }

    public Object getConfig(String key){
        return configs.getOrDefault(key,null);
    }
}
