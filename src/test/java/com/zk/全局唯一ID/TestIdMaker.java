/*
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 */
package com.zk.全局唯一ID;

/***
 * @ClassName: TestIdMaker
 * @Description:
 * @Auther: cxy
 * @Date: 2019/1/30:17:15
 * @version : V1.0
 */
public class TestIdMaker {
    public static void main(String[] args) throws Exception {

        IdMaker idMaker = new IdMaker();
        for (int i = 0; i < 10111; i++) {
            String id = idMaker.generateId(IdMaker.RemoveMethod.NONE);
            System.out.println(id);

        }
    }
}