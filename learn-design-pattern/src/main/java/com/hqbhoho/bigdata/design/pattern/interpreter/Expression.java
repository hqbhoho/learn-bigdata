package com.hqbhoho.bigdata.design.pattern.interpreter;

import java.util.HashMap;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/07
 */
public interface Expression {
    int interpreter(HashMap<String,Integer> map);
}
