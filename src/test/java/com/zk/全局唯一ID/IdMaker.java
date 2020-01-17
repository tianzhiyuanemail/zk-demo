/*
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 */
package com.zk.全局唯一ID;


import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

/**
 * @Author cxy
 * @Description //TODO
 * @Date 2019/1/30
 **/
public class IdMaker {

    private final String root = "/NameService/IdGen";//记录父节点的路径
    private final String nodeName = "ID";//节点的名称

    //删除节点的级别
    public enum RemoveMethod {
        NONE, IMMEDIATELY, DELAY
    }


    static CuratorFramework zkclient = null;


    static {
        String zkhost = "47.92.146.108:2181,47.92.146.108:2182,47.92.146.108:2183";// zk的host
        RetryPolicy rp = new ExponentialBackoffRetry(1000, 3);// 重试机制
        zkclient = CuratorFrameworkFactory.newClient(zkhost, 5000, 5000, rp);
        zkclient.start();// 放在这前面执行
    }


    private String ExtractId(String str) {
        int index = str.lastIndexOf(nodeName);
        if (index >= 0) {
            index += nodeName.length();
            return index <= str.length() ? str.substring(index) : "";
        }
        return str;

    }

    /**
     * 产生ID
     * 核心函数
     *
     * @param removeMethod 删除的方法
     * @return
     * @throws Exception
     */
    public String generateId(RemoveMethod removeMethod) throws Exception {
        final String fullNodePath = root.concat("/").concat(nodeName);
        //返回创建的节点的名称
        //final String ourPath = client.createPersistentSequential(fullNodePath, null);
        final String ourPath = zkclient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(fullNodePath);

        System.out.println(ourPath);

        /**
         * 在创建完节点后为了不占用太多空间，可以选择性删除模式
         */
        if (removeMethod.equals(RemoveMethod.IMMEDIATELY)) {
            zkclient.delete().forPath(ourPath);
        } else if (removeMethod.equals(RemoveMethod.DELAY)) {

        }
        //node-0000000000, node-0000000001，ExtractId提取ID
        return ExtractId(ourPath);
    }

}