package com.hqbhoho.bigdata.design.pattern.flyweight;

import java.util.HashMap;
import java.util.Map;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/03
 */
public class GoChessFlyWeightFactory {
    private Map<String,ChessFlyWeight> chessFlyWeightMap;

    public GoChessFlyWeightFactory() {
        this.chessFlyWeightMap = new HashMap<>();
    }

    public ChessFlyWeight getChessFlyWeight(String color){
        ChessFlyWeight result = chessFlyWeightMap.getOrDefault(color, null);
        if(result == null){
            result = new GoChessFlyWeight(color);
            chessFlyWeightMap.put(color,result);
        }
        return result;
    }
}
