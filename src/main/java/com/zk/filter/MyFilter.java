/*
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 */
package com.zk.filter;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.HttpServletRequest;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.retry.ExponentialBackoffRetry;

@WebFilter(filterName = "myFilter", urlPatterns = "/*")
public class MyFilter implements Filter {

    //zookeeper集群地址
    public static final String ZOOKEEPERSTRING = "192.168.99.129:2181,192.168.99.153:2181,192.168.99.171:2181";

    //zookeeper链接client
    CuratorFramework client = null;

    //选举类
    LeaderSelector leaderSelector;
    String dataPath = "/tomcat/leader";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        //初始化连接
        client = CuratorFrameworkFactory.newClient(ZOOKEEPERSTRING, new ExponentialBackoffRetry(1000, 3));

        //启动一个客户端
        client.start();


        //实例化一个选举器//特别注意LeaderListener，
        // 如果takeLeadership方法执行完毕，则会自动释放leaders身份，故我们使用countDownLatch来阻塞此方法使其不主动放弃leaders身份
        //同时这也给我们一个思路，我们可以通过在外部修改countDownLatch的值来控制leader是否主动放弃其身份
        //放弃leader权限后还可以参加选举
        leaderSelector = new LeaderSelector(client, dataPath, new LeaderListener());
        leaderSelector.autoRequeue();

        //开始服务
        leaderSelector.start();

    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
                         FilterChain chain) throws IOException, ServletException {
        HttpServletRequest req = (HttpServletRequest) request;

        //判断是否现在是leader
        if (leaderSelector.hasLeadership()) {
            chain.doFilter(request, response);
        } else {
            response.getWriter().write("is not servicing");
        }
        //chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
        // TODO Auto-generated method stub

    }

}