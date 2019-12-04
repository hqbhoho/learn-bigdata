package com.hqbhoho.bigdata.design.pattern.adapter.class_adapter;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/02
 */
public class TestDemo {
    public static void main(String[] args) {
        Phone phone = new Phone();
        ChinaNormalVoltageAdapter chinaNormalVoltageAdapter = new ChinaNormalVoltageAdapter();
        phone.charging(chinaNormalVoltageAdapter);
    }
}
