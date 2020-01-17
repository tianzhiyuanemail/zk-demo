/*
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 */
package com.zk.zookeeper.lock;

import com.zk.config.ZkApi;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * zk实现公平/非公平分布式锁
 *
 * @author zc.ding
 * @since 2019/3/23
 */
@Component
@DependsOn({"zkConfig"})
public class DistributedLock {

    private static final Logger LOG = LoggerFactory.getLogger(DistributedLock.class);
    private static final ThreadLocal<String> THREAD_LOCAL = new ThreadLocal<>();
    private static final ThreadLocal<ZooKeeper> ZK_THREAD_LOCAL = new ThreadLocal<>();
    /**
     * 锁的根路径
     **/
    private static final String LOCK_ROOT_PATH = "/locks";
    /**
     * 锁后缀
     **/
    private static final String LOCK_SUFFIX = "_NO_";
    /**
     * 创建根节点同步锁
     **/
    private static final String CREATE_ROOT_LOCK = "LOCK";
    /**
     * 公平锁
     **/
    private static final int LOCK_FAIR = 1;
    private final static byte[] BUF = new byte[0];

    @Autowired
    private ZkApi zkApi;
    @Autowired
    private ZooKeeper zooKeeper;

    private static DistributedLock distributedLock;

    /**
     * 获得所有
     *
     * @param key 路径不含‘/’
     * @return boolean
     * @author ：zc.ding@foxmail.com
     * @since ：2019/3/23
     */
    public static boolean tryLock(String key) {
        return tryLock(key, Constants.LOCK_EXPIRES, Constants.LOCK_WAITTIME);
    }

    /**
     * 获得锁
     *
     * @param key    键/路径
     * @param expire 过期时间
     * @param wait   等待时间
     * @return boolean
     * @author ：zc.ding@foxmail.com
     * @since ：2019/3/23
     */
    public static boolean tryLock(String key, long expire, long wait) {
        ZooKeeper zooKeeper = distributedLock.zooKeeper;
        ZK_THREAD_LOCAL.set(zooKeeper);
        return tryLock(zooKeeper, key, expire, wait);
    }

    /**
     * 获得锁
     *
     * @param zooKeeper zk连接
     * @param key       路径
     * @param expire    过期时间
     * @param wait      等待时间
     * @return boolean
     * @author ：zc.ding@foxmail.com
     * @since ：2019/3/23
     */
    private static boolean tryLock(ZooKeeper zooKeeper, String key, long expire, long wait) {
        expire = expire * 1000;
        wait = wait * 1000;
        final String currNode = "1";
        String path = LOCK_ROOT_PATH + "/" + key + LOCK_SUFFIX;
        try {
            Stat exists = zooKeeper.exists(LOCK_ROOT_PATH, false);
//            distributedLock.zkApi.createNode(LOCK_ROOT_PATH, "1", CreateMode.PERSISTENT);
//            distributedLock.zkApi.createNode(path, "1", CreateMode.PERSISTENT);
//            //zkClient.create("/zk-z-1", "1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//
//            String data = distributedLock.zkApi.getData("/zk-z-1", new WatcherApi());
//            // byte[] bytes = zkClient.getData("/zk-z-1", new WatcherApi(), new Stat());
//
//            //String s = new String(bytes);
//
//            System.out.println("打印的值为：" + data);
//            distributedLock.zkApi.deleteNode("/zk-z-1");

            zooKeeper.create(path, "1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            //zooKeeper.create(path, BUF, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            //步骤一
            List<String> nodes = zooKeeper.getChildren(LOCK_ROOT_PATH, false);
            //过滤掉集合中不是当前业务的临时节点
            nodes = nodes.stream().filter(o -> o.startsWith(key)).collect(Collectors.toList());
            nodes.sort(String::compareTo);
            //如果集合为空说明当前创建节点的session在步骤一处已经断开，并且创建的节点已经被zk服务器删除, 此种情况比较极端
            if (nodes.size() == 0) {
                return false;
            }
            //最小的节点就是自己创建的节点表示拿到锁
            if (currNode.endsWith(nodes.get(0))) {
                runExpireThread(zooKeeper, currNode, expire);
                return true;
            }
            //没有拿到锁
            CountDownLatch countDownLatch = new CountDownLatch(1);
            //非公平锁
            if (1 == LOCK_FAIR) {
                for (int i = 0; i < nodes.size(); i++) {
                    String node = nodes.get(i);
                    if (currNode.endsWith(node)) {
                        runExpireThread(zooKeeper, currNode, expire);
                        return true;
                    }
                    Stat stat = zooKeeper.exists(LOCK_ROOT_PATH + "/" + node, new LockWatcher(countDownLatch));
                    if (stat != null) {
                        delPath(zooKeeper);
                        //等待锁超时
                        if (!countDownLatch.await(wait, TimeUnit.MILLISECONDS)) {
                            return tryLock(zooKeeper, key, expire, wait);
                        }
                    }
                }
            } else {
                for (int i = 0; i < nodes.size(); i++) {
                    String node = nodes.get(i);
                    if (currNode.endsWith(node)) {
                        runExpireThread(zooKeeper, currNode, expire);
                        return true;
                    }
                    //当前节点的前一个节点
                    if (currNode.endsWith(nodes.get(i + 1))) {
                        Stat stat = zooKeeper.exists(LOCK_ROOT_PATH + "/" + node, new LockWatcher(countDownLatch));
                        if (stat != null) {
                            // 等待锁超时，如果是公平锁，等待时间是默认等待时间的2倍，防止因为拿锁的线程处理业务时间太久
                            // 导致当前线程等待超时
                            if (!countDownLatch.await(wait * 2, TimeUnit.MILLISECONDS)) {
                                delPath(zooKeeper);
                                return false;
                            }
                            return true;
                        }
                    }
                }
            }
        } catch (KeeperException | InterruptedException e) {
            LOG.error("create '{}' node fail.", key, e);
        }
        return false;
    }

    /**
     * 释放锁
     *
     * @author ：zc.ding@foxmail.com
     * @since ：2019/3/23
     */
    public static void unLock() {
        ZooKeeper zooKeeper = ZK_THREAD_LOCAL.get();
        delPath(zooKeeper);
        close(ZK_THREAD_LOCAL.get());
        THREAD_LOCAL.remove();
        ZK_THREAD_LOCAL.remove();
    }

    /**
     * 创建分布式锁的根路径
     *
     * @author ：zc.ding@foxmail.com
     * @since ：2019/3/23
     */
    private static void createLockRootPath() {
        ZooKeeper zooKeeper = distributedLock.zooKeeper;
        try {
            Stat stat = zooKeeper.exists(LOCK_ROOT_PATH, false);
            if (stat == null) {
                synchronized (CREATE_ROOT_LOCK) {
                    stat = zooKeeper.exists(LOCK_ROOT_PATH, false);
                    if (stat == null) {
                        LOG.info("create lock root path '{}'", LOCK_ROOT_PATH);
                        zooKeeper.create(LOCK_ROOT_PATH, BUF, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT_SEQUENTIAL);
                    }
                }
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }


    /**
     * 启动一个线程来判断锁的过期时间，方式业务假死，zk不断开导致死锁
     *
     * @param zooKeeper zk连接
     * @param currNode  当前节点
     * @author ：zc.ding@foxmail.com
     * @since ：2019/3/23
     */
    private static void runExpireThread(final ZooKeeper zooKeeper, String currNode, long expire) {
        THREAD_LOCAL.set(currNode);

//        Executors.newScheduledThreadPool(1).execute(() -> {
//            try {
//                Thread.sleep(expire * 1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            LOG.info("等待了{}秒, 主动结束.", expire);
//            delPath(zooKeeper);
//        });

    }

    /**
     * 删除创建的路径
     *
     * @param zooKeeper zk连接
     * @author ：zc.ding@foxmail.com
     * @since ：2019/3/23
     */
    private static void delPath(ZooKeeper zooKeeper) {
        try {
            //无论节点是否存在，直接执行删除操作
            zooKeeper.delete(THREAD_LOCAL.get(), -1);
        } catch (Exception e) {
            LOG.error("lock expire, delete lock");
        }
    }

    /**
     * 断开连接
     *
     * @param zooKeeper zk连接
     * @author ：zc.ding@foxmail.com
     * @since ：2019/3/23
     */
    private static void close(ZooKeeper zooKeeper) {
        if (zooKeeper != null) {
            try {
                zooKeeper.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @PostConstruct
    public void afterProperties() {
        distributedLock = this;
        distributedLock.zkApi = this.zkApi;
        distributedLock.zooKeeper = this.zooKeeper;
    }

    /**
     * 监听节点删除事件
     *
     * @author ：zc.ding@foxmail.com
     * @since ：2019/3/23
     */
    static class LockWatcher implements Watcher {
        private CountDownLatch latch;

        public LockWatcher(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeDeleted) {
                latch.countDown();
            }
        }
    }

    private static class Constants {
        public static Integer LOCK_EXPIRES = 10;
        public static Integer LOCK_WAITTIME = 10;

    }
}