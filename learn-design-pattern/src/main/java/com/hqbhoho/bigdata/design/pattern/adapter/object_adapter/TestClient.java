package com.hqbhoho.bigdata.design.pattern.adapter.object_adapter;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/02
 */
public class TestClient {
    public static void main(String[] args) {
        NormalVoltageAdapterMapping normalVoltageAdapterMapping = new NormalVoltageAdapterMapping();
        NormalVoltageAdapter chinaNormalVoltageAdapter = new ChinaNormalVoltageAdapter();
        NormalVoltageAdapter janpaNormalVoltageAdapter = new JanpaNormalVoltageAdapter();
        normalVoltageAdapterMapping.register(chinaNormalVoltageAdapter);
        normalVoltageAdapterMapping.register(janpaNormalVoltageAdapter);
        Phone phone = new Phone();

        System.out.println("======China=======");
        NormalVoltage chinaNormalVoltage = new ChinaNormalVoltage();
        System.out.println("NormalVoltage: "+ chinaNormalVoltage.outputVoltage());
        NormalVoltageAdapter normalVoltageAdapter1 = normalVoltageAdapterMapping.getNormalVoltageAdapter(chinaNormalVoltage);
        phone.charging(normalVoltageAdapter1);

        System.out.println("======Janpa=======");
        NormalVoltage janpaNormalVoltage = new JanpaNormalVoltage();
        System.out.println("NormalVoltage: "+ janpaNormalVoltage.outputVoltage());
        NormalVoltageAdapter normalVoltageAdapter2 = normalVoltageAdapterMapping.getNormalVoltageAdapter(janpaNormalVoltage);
        phone.charging(normalVoltageAdapter2);


    }
}
