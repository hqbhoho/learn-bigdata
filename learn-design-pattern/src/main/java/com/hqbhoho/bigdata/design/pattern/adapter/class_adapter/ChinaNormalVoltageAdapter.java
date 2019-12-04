package com.hqbhoho.bigdata.design.pattern.adapter.class_adapter;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/02
 */
public class ChinaNormalVoltageAdapter extends ChinaNormalVoltage implements NormalVoltageVoltage {
    @Override
    public int output5V() {
        return super.outputVoltage() / 44;
    }
}
