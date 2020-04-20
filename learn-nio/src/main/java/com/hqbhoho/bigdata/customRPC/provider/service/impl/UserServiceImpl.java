package com.hqbhoho.bigdata.customRPC.provider.service.impl;

import com.hqbhoho.bigdata.customRPC.service.UserService;
import com.hqbhoho.bigdata.customRPC.service.pojo.User;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/19
 */
public class UserServiceImpl implements UserService {

    private HashMap<Integer, User> idData = new HashMap();
    private HashMap<String, User> nameData = new HashMap();

    public UserServiceImpl() {
        this.idData.put(1, new User("hqbhoho1", 1, "1"));
        this.idData.put(2, new User("hqbhoho2", 1, "1"));
        this.nameData.put("hqbhoho1", new User("hqbhoho1", 1, "1"));
        this.nameData.put("hqbhoho2", new User("hqbhoho2", 1, "1"));
    }

    @Override
    public User getUserById(int id) {
        System.err.println("method getUserById invoked....");
        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return idData.get(id);
    }

    @Override
    public User getUserByName(String name)
    {
        System.err.println("method getUserByName invoked....");
        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return nameData.get(name);
    }
}
