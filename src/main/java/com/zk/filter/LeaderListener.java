/*
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 */
package com.zk.filter;

import java.util.concurrent.CountDownLatch;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

public class LeaderListener extends LeaderSelectorListenerAdapter{
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    //当这个方法执行完毕以后，leader会自动放弃身份，我们在这个项目当中使用countDownLatch阻塞不主动放弃身份
    //这里给我们提供思路，我们可以在外部动态修改countDownLatch值来使其主动放弃leader身份
    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        System.out.println("一直阻塞当中，一直提供服务");
        
        /**
         * 使用countdownLath，使leader永远不放弃自己的地位
         */
        countDownLatch.await();;
    }


}