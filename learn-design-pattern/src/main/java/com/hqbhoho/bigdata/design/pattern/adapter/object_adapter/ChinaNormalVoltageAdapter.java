package com.hqbhoho.bigdata.design.pattern.adapter.object_adapter;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/02
 */
public class ChinaNormalVoltageAdapter implements NormalVoltageAdapter {
    private ChinaNormalVoltage chinaNormalVoltage = new ChinaNormalVoltage();
    @Override
    public boolean support(NormalVoltage normalVoltage) {
        return normalVoltage instanceof ChinaNormalVoltage;
    }

    @Override
    public int output5V() {
        return chinaNormalVoltage.outputVoltage() / 44;
    }
}
