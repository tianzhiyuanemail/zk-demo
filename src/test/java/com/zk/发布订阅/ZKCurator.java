package com.zk.发布订阅;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.RetryNTimes;

public class ZKCurator {
    private static final String ADDR = "47.92.146.108:2181,47.92.146.108:2182,47.92.146.108:2183";
    private static final String PATH = "/yuan";

    public static void main(String[] args) throws InterruptedException {
        final CuratorFramework zkClient = CuratorFrameworkFactory.newClient(ADDR, new RetryNTimes(10, 5000));
        zkClient.start();
        System.out.println("start zkclient...");

        try {
            registerWatcher(zkClient);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("注册成功");

        Thread.sleep(Integer.MAX_VALUE);
        zkClient.close();
    }


    /**
     * PathChildrenCache  对指定路径节点的一级子目录监听，不对该节点的操作监听，对其子目录的增删改操作监听
     * <p>
     * 注册监听器，当前节点不存在，创建该节点：未抛出异常及错误日志
     * 注册子节点触发  type=[CHILD_ADDED]
     * 更新触发       type=[CHILD_UPDATED]
     * zk挂掉        type=CONNECTION_SUSPENDED, 一段时间后 type=CONNECTION_LOST
     * 重启zk：      type=CONNECTION_RECONNECTED, data=null
     * 更新子节点：   type=CHILD_UPDATED
     * data=ChildData{path='/zktest111/tt1', stat=4294979983,4294979993,1501037475236,1501037733805,2,0,0,0,6,0,4294979983, data=[55, 55, 55, 55, 55, 55]}
     * 删除子节点type=CHILD_REMOVED
     * <p>
     * 更新根节点：不触发
     * 删除根节点：不触发  无异常
     * 创建根节点：不触发
     * 再创建及更新子节点不触发
     * <p>
     * 重启时，与zk连接失败
     */
    private static void registerWatcher(CuratorFramework zkClient) throws Exception {

        PathChildrenCache watcher = new PathChildrenCache(zkClient, PATH, true);
        watcher.getListenable().addListener(new PathChildrenCacheListener() {
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                System.out.println(pathChildrenCacheEvent);
                switch (pathChildrenCacheEvent.getType()) {
                    case INITIALIZED:
                        System.out.println("监听 INITIALIZED");
                        break;
                    case CHILD_ADDED:
                        System.out.println("监听 CHILD_ADDED");
                        break;
                    case CHILD_REMOVED:
                        System.out.println("监听 CHILD_REMOVED");
                        break;
                    case CHILD_UPDATED:
                        System.out.println("监听 CHILD_UPDATED");
                        break;
                    case CONNECTION_LOST:
                        System.out.println("监听 CONNECTION_LOST");
                        break;
                    case CONNECTION_SUSPENDED:
                        System.out.println("监听 CONNECTION_SUSPENDED");
                        break;
                    case CONNECTION_RECONNECTED:
                        System.out.println("监听 CONNECTION_RECONNECTED");
                        break;
                }
            }
        });

        watcher.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        System.out.println("注册watcher成功...");
    }

    /**
     * NodeCache  对一个节点进行监听，监听事件包括指定路径的增删改操作
     * <p>
     * 节点路径不存在时，set不触发监听
     * 节点路径不存在，，，创建事件触发监听
     * 节点路径存在，set触发监听
     * 节点路径存在，delete触发监听
     * <p>
     * <p>
     * 节点挂掉，未触发任何监听
     * 节点重连，未触发任何监听
     * 节点重连 ，恢复监听
     */
    public static void registerNodeCache(CuratorFramework client) throws Exception {

        final NodeCache nodeCache = new NodeCache(client, PATH, false);
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            public void nodeChanged() throws Exception {
                System.out.println("当前节点：" + nodeCache.getCurrentData());
            }
        });
        //如果为true则首次不会缓存节点内容到cache中，默认为false,设置为true首次不会触发监听事件
        nodeCache.start(true);
    }

    /**
     * TreeCache  综合NodeCache和PathChildrenCahce的特性，是对整个目录进行监听，可以设置监听深度。
     * <p>
     * TreeCache.nodeState == LIVE的时候，才能执行getCurrentChildren非空,默认为PENDING
     * 初始化完成之后，监听节点操作时 TreeCache.nodeState == LIVE
     * <p>
     * maxDepth值设置说明，比如当前监听节点/t1，目录最深为/t1/t2/t3/t4,则maxDepth=3,说明下面3级子目录全
     * 监听，即监听到t4，如果为2，则监听到t3,对t3的子节点操作不再触发
     * maxDepth最大值2147483647
     * <p>
     * 初次开启监听器会把当前节点及所有子目录节点，触发[type=NODE_ADDED]事件添加所有节点（小等于maxDepth目录）
     * 默认监听深度至最低层
     * 初始化以[type=INITIALIZED]结束
     * <p>
     * [type=NODE_UPDATED],set更新节点值操作，范围[当前节点，maxDepth目录节点](闭区间)
     * [type=NODE_ADDED] 增加节点 范围[当前节点，maxDepth目录节点](左闭右闭区间)
     * [type=NODE_REMOVED] 删除节点， 范围[当前节点， maxDepth目录节点](闭区间),删除当前节点无异常
     * <p>
     * 事件信息
     * TreeCacheEvent{type=NODE_ADDED, data=ChildData{path='/zktest1',stat=4294979373,4294979373,1499850881635,1499850881635,0,0,0,0,2,0,4294979373, data=[116, 49]}}
     */
    public static void registTreeCache(CuratorFramework client) throws Exception {
        final TreeCache treeCache = TreeCache.newBuilder(client, PATH).setCacheData(true).setMaxDepth(2).build();
        treeCache.getListenable().addListener(new TreeCacheListener() {
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                System.out.println("treeCacheEvent: " + treeCacheEvent);
                switch (treeCacheEvent.getType()) {
                    case NODE_ADDED:
                        System.out.println("子节点添加");
                        break;
                    case NODE_REMOVED:
                        System.out.println("子节点删除");
                        break;
                    case NODE_UPDATED:
                        System.out.println("子节点修改");
                        break;
                    case INITIALIZED:
                        System.out.println("子节点INITIALIZED");
                        break;
                    default:
                        System.out.println("default");
                }
            }
        });
        //没有开启模式作为入参的方法
        treeCache.start();
    }
}