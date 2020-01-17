/*
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 */
package com.zk.全局唯一ID;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.util.List;

public class CuratorOnlyId {
    static CuratorFramework zkclient = null;
    static String nameSpace = "mo";

    static {
        String zkhost = "47.92.146.108:2181,47.92.146.108:2182,47.92.146.108:2183";// zk的host
        RetryPolicy rp = new ExponentialBackoffRetry(1000, 3);// 重试机制
        zkclient = CuratorFrameworkFactory.newClient(zkhost,5000,5000,rp);
        zkclient.start();// 放在这前面执行
    }

    public static void main(String[] args) throws Exception {
        create();
    }

    /**
     * * 监听节点变化      *
     */
    public static void create() throws Exception {
//        String s = zkclient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/only", "12".getBytes());
//        System.out.println(s);

        for (int i = 0; i < 100; i++) {
            String s1 = zkclient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/only/tb-");
            System.out.println(s1);
            zkclient.delete().forPath(s1);
        }

    }




}