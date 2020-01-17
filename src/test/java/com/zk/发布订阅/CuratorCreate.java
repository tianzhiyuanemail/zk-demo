/*
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 */
package com.zk.发布订阅;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class CuratorCreate {
    static CuratorFramework zkclient = null;
    static String nameSpace = "mo";

    static {
        String zkhost = "47.92.146.108:2181,47.92.146.108:2182,47.92.146.108:2183";// zk的host
        RetryPolicy rp = new ExponentialBackoffRetry(1000, 3);// 重试机制
        zkclient = CuratorFrameworkFactory.newClient(zkhost,5000,5000,rp);
//        zkclient = CuratorFrameworkFactory
//                .builder()
//                .connectString(zkhost)
//                .connectionTimeoutMs(5000)
//                .sessionTimeoutMs(5000)
//                .retryPolicy(rp)
//                .namespace(nameSpace)
//                .build()
//                ;
        zkclient.start();// 放在这前面执行
    }

    public static void main(String[] args) throws Exception {
        create();
    }

    /**
     * * 监听节点变化      *
     */
    public static void create() throws Exception {
//        zkclient.delete().deletingChildrenIfNeeded().forPath("/ab");
        //        zkclient.create().creatingParentsIfNeeded().forPath("/","12".getBytes());
        zkclient.create().creatingParentsIfNeeded().forPath("/yuan/ab","12".getBytes());
//        zkclient.setData().forPath("/ab","34".getBytes());
//        zkclient.create().creatingParentsIfNeeded().forPath("/ab/cd","12".getBytes());
//        zkclient.create().creatingParentsIfNeeded().forPath("/ab/qw","12".getBytes());
//        zkclient.create().creatingParentsIfNeeded().forPath("/ab/qq","12".getBytes());

        Thread.sleep(2000);

        zkclient.delete().deletingChildrenIfNeeded().forPath("/yuan");

    }




}