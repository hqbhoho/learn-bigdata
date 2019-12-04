package com.hqbhoho.bigdata.design.pattern.adapter.object_adapter;

public interface NormalVoltageAdapter {
    boolean support(NormalVoltage normalVoltage);
    int output5V();
}
