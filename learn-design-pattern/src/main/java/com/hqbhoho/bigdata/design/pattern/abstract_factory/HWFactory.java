package com.hqbhoho.bigdata.design.pattern.abstract_factory;

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

    @Override
    public Computer productComputer(String name) {
        Computer c = null;
        switch (name){
            case "1": c = new HWComputer1("Y460","HW",2000.0); break;
        }
        return c;
    }
}
