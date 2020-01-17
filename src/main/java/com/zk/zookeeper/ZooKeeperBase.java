/*
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 */
package com.zk.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 为 ZooKeeper测试代码创建一个基类，封装建立连接的过程
 *
 * @author junyongliao
 * @date 2019/8/16
 */
public class ZooKeeperBase implements Watcher {
    /**
     * 日志，不使用 @Slf4j ，是要使用子类的log
     */
    Logger log = null;

    /**
     * 等待连接建立成功的信号
     */
    private CountDownLatch connectedSemaphore = new CountDownLatch(1);
    /**
     * ZooKeeper 客户端
     */
    private ZooKeeper zooKeeper = null;
    /**
     * 避免重复根节点
     */
    static Integer rootNodeInitial = Integer.valueOf(1);

    /**
     * 收到的所有Event
     */
    List<WatchedEvent> watchedEventList = new ArrayList<>();

    /**
     * 构造函数
     */
    public ZooKeeperBase(String address) throws IOException {
        log = LoggerFactory.getLogger(getClass());

        try {
            final CountDownLatch countDownLatch = new CountDownLatch(1);
            //连接成功后，会回调watcher监听，此连接操作是异步的，执行完new语句后，直接调用后续代码
            //  可指定多台服务地址 127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183
            zooKeeper = new ZooKeeper("47.92.146.108:2181,47.92.146.108:2182,47.92.146.108:2183", 4000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (Event.KeeperState.SyncConnected == event.getState()) {
                        //如果收到了服务端的响应事件,连接成功
                        countDownLatch.countDown();
                    }
                }
            });
            countDownLatch.await();
            log.info("【初始化ZooKeeper连接状态....】={}", zooKeeper.getState());

        } catch (InterruptedException e) {
            log.error("InterruptedException", e);
        }

    }

    /**
     * 创建测试需要的根节点
     *
     * @param rootNodeName
     *
     * @return
     */
    public String createRootNode(String rootNodeName) {
        CreateMode createMode = CreateMode.PERSISTENT;
        return createRootNode(rootNodeName, createMode);
    }

    /**
     * 创建测试需要的根节点，需要指定 CreateMode
     *
     * @param rootNodeName
     * @param createMode
     *
     * @return
     */
    public String createRootNode(String rootNodeName, CreateMode createMode) {
        synchronized(rootNodeInitial) {
            // 创建 tableSerial 的zNode
            try {
                Stat existsStat = getZooKeeper().exists(rootNodeName, false);
                if (existsStat == null) {
                    rootNodeName = getZooKeeper().create(rootNodeName, new byte[0],
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
                }
            } catch (KeeperException e) {
                log.info("创建节点失败，可能是其他客户端已经创建", e);
            } catch (InterruptedException e) {
                log.error("InterruptedException", e);
            }
        }
        return rootNodeName;
    }

    /**
     * 读取ZooKeeper对象，供子类调用
     */
    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    @Override
    final public void process(WatchedEvent event) {
        if (Event.EventType.None.equals(event.getType())) {
            // 连接状态发生变化
            if (Event.KeeperState.SyncConnected.equals(event.getState())) {
                // 连接建立成功
                connectedSemaphore.countDown();
            }
        } else {
            watchedEventList.add(event);

            if (Event.EventType.NodeCreated.equals(event.getType())) {
                processNodeCreated(event);
            } else if (Event.EventType.NodeDeleted.equals(event.getType())) {
                processNodeDeleted(event);
            } else if (Event.EventType.NodeDataChanged.equals(event.getType())) {
                processNodeDataChanged(event);
            } else if (Event.EventType.NodeChildrenChanged.equals(event.getType())) {
                processNodeChildrenChanged(event);
            }
        }
    }

    /**
     * 处理事件: NodeCreated
     *
     * @param event
     */
    protected void processNodeCreated(WatchedEvent event) {
    }

    /**
     * 处理事件: NodeDeleted
     *
     * @param event
     */
    protected void processNodeDeleted(WatchedEvent event) {
    }

    /**
     * 处理事件: NodeDataChanged
     *
     * @param event
     */
    protected void processNodeDataChanged(WatchedEvent event) {
    }

    /**
     * 处理事件: NodeChildrenChanged
     *
     * @param event
     */
    protected void processNodeChildrenChanged(WatchedEvent event) {
    }

    /**
     * 收到的所有 Event 列表
     */
    public List<WatchedEvent> getWatchedEventList() {
        return watchedEventList;
    }
}
