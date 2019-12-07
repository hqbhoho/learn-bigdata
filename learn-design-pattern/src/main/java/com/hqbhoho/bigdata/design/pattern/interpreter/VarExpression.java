package com.hqbhoho.bigdata.design.pattern.interpreter;

import java.util.HashMap;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/07
 */
public class VarExpression implements Expression{
    private String key;

    public VarExpression(String key) {
        this.key = key;
    }

    @Override
    public int interpreter(HashMap<String, Integer> map) {
        return map.get(this.key);
    }
}
