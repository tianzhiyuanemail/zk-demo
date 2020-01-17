/*
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 */
package com.zk.master选举;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;

public class T6 {

    private static final String PATH = "/master";

    private static final String IP_PORT = "47.92.146.108:2181,47.92.146.108:2182,47.92.146.108:2183";

    private static final int SESSION_TIMEOUT = 10000;

    private static final int CONNECT_TIMEOUT = 10000;


    /**
     * 重试策略:重试间隔时间为1000ms; 最多重试3次;
     */
    private static RetryPolicy retryPolicy = new RetryNTimes(3, 1000);

    public static void main(String[] args) throws Exception {
        //zk的重连策略
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        //获取连接
        CuratorFramework client = CuratorFrameworkFactory.newClient(IP_PORT, retryPolicy);
        client.start();

        String path = "/newserver/leader";  //选举的节点信息放在这个path下

        LeaderLatch leaderLatch = new LeaderLatch(client, path, "1");

        // 添加监听器
        leaderLatch.addListener(new LeaderLatchListener() {

            @Override
            public void isLeader() {

            }

            @Override
            public void notLeader() {

            }
        });

        leaderLatch.start();


        System.in.read();
    }
}
