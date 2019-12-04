package com.hqbhoho.bigdata.design.pattern.adapter.object_adapter;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/02
 */
public class JanpaNormalVoltageAdapter implements NormalVoltageAdapter {
    private JanpaNormalVoltage janpaNormalVoltage = new JanpaNormalVoltage();
    @Override
    public boolean support(NormalVoltage normalVoltage) {
        return normalVoltage instanceof JanpaNormalVoltage;
    }

    @Override
    public int output5V() {
        return janpaNormalVoltage.outputVoltage() / 22;
    }
}
