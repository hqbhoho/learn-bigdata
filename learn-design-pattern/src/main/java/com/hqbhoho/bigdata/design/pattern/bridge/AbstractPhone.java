package com.hqbhoho.bigdata.design.pattern.bridge;

import java.util.ArrayList;
import java.util.List;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/02
 */
public abstract class AbstractPhone implements Phone{
    private List<Software> softwares;

    public AbstractPhone() {
        this.softwares = new ArrayList<>();
    }

    @Override
    public void installSoftware(Software software){
        this.softwares.add(software);
    }

    @Override
    public void listSoftware(){
        for(Software software :softwares){
            software.action();
        }
    }

    @Override
    public void showMe(){
        showBrand();
        listSoftware();
    }
}
