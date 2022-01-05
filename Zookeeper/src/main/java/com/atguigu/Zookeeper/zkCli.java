package com.atguigu.Zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author CZY
 * @date 2021/10/24 16:40
 * @description zkCli
 */
public class zkCli {

    private ZooKeeper zkClient;

    /**
     * 初始化客户端
     * 参数：
     * connectString 集群连接字符串
     * sessionTimeout 连接超时时间 单位:毫秒
     * Watcher  当前客户端默认的监控器
     */
    @Before
    public void init() throws IOException {
        String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        int sessionTimeout =10000;

        //Listener线程内部回调process方法
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //Listener线程内部回调process方法
            }
        });
    }

    @After
    public void close() throws InterruptedException {
        zkClient.close();
    }

    @Test
    public void ls() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", false);
        System.out.println(children);
    }

    @Test
    //获取子节点列表,并监听
    public void lsAndWatch() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/sanguo", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
            }
        });
        System.out.println(children);
        //因为设置了监听,所以当前线程不能结束
        Thread.sleep(Long.MAX_VALUE);

    }

    @Test
    /**
     * 创建子节点
     * 参数详解：
     * path 节点路径
     * data 节点存储的数据
     * acl  节点的权限(使用Ids选个OPEN即可)
     * CreateMode 节点类型 短暂 持久 短暂带序号 持久带序号
     */
    public void create() throws KeeperException, InterruptedException {
        //创建节点
        /*String path = zkClient.create("/huajianghu", "jianghu".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);*/
        //创建临时节点
        String path2 = zkClient.create("/huajianghu2", "jianghu".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        //System.out.println(path);
        //创建临时节点的话,需要线程阻塞
        Thread.sleep(10000);
        System.out.println(path2);

    }

    @Test
    public void exist() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/huajianghu", false);
        System.out.println(stat == null ? "not exist":"exits");
    }

    @Test
    public void get() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/huajianghu", false);
        if (stat == null){
            System.out.println("节点不存在");
            return;
        }
        byte[] data = zkClient.getData("/huajianghu", false, stat);
        System.out.println(new String(data));
    }

    @Test
    //获取子节点存储的数据,并监听
    public void getAndWatch() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/huajianghu", false);
        if (stat == null){
            System.out.println("节点不存在");
        }
        byte[] data = zkClient.getData("/huajianghu", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
            }
        }, stat);
        System.out.println(new String(data));
        //线程阻塞
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    //参数解读 1节点路径 2节点的值 3版本号
    public void set() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/huajianghu", false);
        if (stat == null){
            System.out.println("节点不存在");
            return;
        }
        zkClient.setData("/huajianghu","beimotin".getBytes(),stat.getVersion());
    }

    @Test
    // 删除空节点
    public void delete() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/sanguo/honglou", false);
        if (stat== null){
            System.out.println("节点不存在");
            return;
        }
        zkClient.delete("/sanguo/honglou",-1);
    }

    //封装一个方法,方便递归调用
    public void deleteAll(String path , ZooKeeper zk) throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists(path, false);
        if (stat== null){
            System.out.println("节点不存在");
            return;
        }
        //先获取当前传入节点下的所有子节点
        List<String> children = zk.getChildren(path, false);
        if (children.isEmpty()){
            zk.delete(path,-1);
        }else {
            for (String child : children) {
                //删除子节点,但是不知道子节点下面还有没有子节点,所以递归调用
                deleteAll(path+"/"+child,zk);
            }
            //删除完所有子节点以后,记得删除传入的节点
            zk.delete(path, stat.getVersion());
        }
    }

    @Test
    //删除非空节点,递归实现
    public void testDeleteAll() throws KeeperException, InterruptedException {
        deleteAll("/sanguo",zkClient);
    }

    @Test
    //获取子节点的列表,并且循环监听
    public void testWatch() throws InterruptedException, KeeperException {
        lsAndWatch0("/huajianghu");
        Thread.sleep(Long.MAX_VALUE);
    }
    //封装一个方法,方便递归调用
    private void lsAndWatch0(String path) throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
                System.out.println("这可能下面还有目录：");
                try {
                    lsAndWatch0(path);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        System.out.println(children);
    }

}
