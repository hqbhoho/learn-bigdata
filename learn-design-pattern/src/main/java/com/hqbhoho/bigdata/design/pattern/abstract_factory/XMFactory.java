package com.hqbhoho.bigdata.design.pattern.abstract_factory;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/11/28
 */
public class XMFactory implements PhoneFactory {
    @Override
    public Phone productPhone(String name) {
        Phone p = null;
        switch (name){
            case "1": p = new XMPhone1("C9","XM",2000.0); break;
            case "2": p = new XMPhone2("Mix3","XM",3999.0);break;
        }
        return p;
    }

    @Override
    public Computer productComputer(String name) {
        Computer c = null;
        switch (name){
            case "1": c = new XMComputer1("M480","XM",3999.0); break;
        }
        return c;
    }
}
