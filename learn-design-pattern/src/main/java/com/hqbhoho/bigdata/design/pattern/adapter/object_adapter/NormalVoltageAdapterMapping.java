package com.hqbhoho.bigdata.design.pattern.adapter.object_adapter;

import java.util.ArrayList;
import java.util.List;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/02
 */
public class NormalVoltageAdapterMapping {

    private List<NormalVoltageAdapter> adapters = new ArrayList<>();

    public void register(NormalVoltageAdapter normalVoltageAdapter){
        adapters.add(normalVoltageAdapter);
    }

    public NormalVoltageAdapter getNormalVoltageAdapter(NormalVoltage normalVoltage){
        NormalVoltageAdapter resNormalVoltageAdapter = null;
        for(NormalVoltageAdapter normalVoltageAdapter : adapters ){
            if(normalVoltageAdapter.support(normalVoltage)){
                resNormalVoltageAdapter = normalVoltageAdapter;
                break;
            }
        }
        if (resNormalVoltageAdapter == null){
            throw new  IllegalArgumentException("没有找到合适的变压适配器");
        }
        return resNormalVoltageAdapter;
    }
}
