/*
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 */
package com.zk.分布式锁;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.zk.zookeeper.lock.DistributedLock;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ZkDemoApplicationTests2 {

    @Test
    public void contextLoads() {

        try {
            int count = 50;
            final Integer[] sum = {0};
            //            ExecutorService executorService = Executors.newFixedThreadPool(count);
            //              executorService.execute(() -> {
            try {
                if (DistributedLock.tryLock("lock")) {
                    System.out.println("我拿到了锁");
                    if (sum[0] == 0) {
                        System.out.println("sum已经是0");
                        return;
                    }
                    System.out.println("还没有停止，sum=" + sum[0]);
                    sum[0] = sum[0] - 1;
                    // System.out.println(Thread.currentThread().getName() + ": 我要等待6秒，测试其他等人超市是否有效.");
                    // Thread.sleep(6000);
                    // System.out.println("我的等待结束了，其他人可以过来拿锁了.");
                } else {
                    System.out.println("zk等的花都谢了!");
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                DistributedLock.unLock();
            }
            //                });
            //            }
            //            executorService.shutdown();
            Thread.sleep(30000);
            System.out.println("finish, sum=" + sum[0]);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
