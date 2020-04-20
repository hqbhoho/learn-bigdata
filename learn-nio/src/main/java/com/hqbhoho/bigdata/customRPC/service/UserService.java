package com.hqbhoho.bigdata.customRPC.service;

import com.hqbhoho.bigdata.customRPC.service.pojo.User;

public interface UserService {
    User getUserById(int id);

    User getUserByName(String name);
}
