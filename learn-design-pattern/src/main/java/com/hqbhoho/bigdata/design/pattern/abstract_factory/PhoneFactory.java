package com.hqbhoho.bigdata.design.pattern.abstract_factory;

public interface PhoneFactory {
    Phone productPhone(String name);
    Computer productComputer(String name);
}
