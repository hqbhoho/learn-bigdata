package com.hqbhoho.bigdata.design.pattern.factory_method;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/11/28
 */
public class HWFactory implements PhoneFactory {
    @Override
    public Phone productPhone(String name) {
        Phone p = null;
        switch (name){
            case "1": p = new HWPhone1("P30","HW",2000.0); break;
            case "2": p = new HWPhone2("Mate20","HW",3999.0);break;
        }
        return p;
    }
}
