package com.hqbhoho.bigdata.design.pattern.adapter.object_adapter;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/02
 */
public class JanpaNormalVoltage implements NormalVoltage {
    private static final int NORMAL_VOLTAGE = 110;

    @Override
    public int outputVoltage() {
        return NORMAL_VOLTAGE;
    }
}
