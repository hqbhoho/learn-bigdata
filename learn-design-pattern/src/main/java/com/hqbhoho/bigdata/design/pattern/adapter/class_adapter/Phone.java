package com.hqbhoho.bigdata.design.pattern.adapter.class_adapter;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/02
 */
public class Phone {
    public void charging(NormalVoltageVoltage phoneVoltage){
        if(phoneVoltage.output5V() == 5){
            System.out.println("充电电压为 5V,正在充电...");
        }else{
            System.out.println("充电电压不为 5V,不能充电...");
        }
    }
}
