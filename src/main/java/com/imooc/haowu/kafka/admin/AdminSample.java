package com.imooc.haowu.kafka.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.internals.Topic;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * @Author haowus919@gmail.com in lab
 * @Date 2020/8/24 20:56
 */
public class AdminSample {
    public final static String TOPIC_NAME = "hw_topic";
    public static void main(String[] args) throws Exception {
//        AdminClient adminClient = AdminSample.adminClient();
//        System.out.println("adminClient: "+adminClient);
//        createTopic();  //创建Topic实例
        delTopics();  //删除topic实例
        topicList();  //获取Topic列表
    }

    /*
    设置AdminClient  客户端对象 172.17.159.58
     */
    public static AdminClient adminClient(){
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"123.56.170.13:9092");
        AdminClient adminClient = AdminClient.create(properties);
        return adminClient;
    }

    /*
    创建Topic实例
     */
    public static void createTopic(){
        AdminClient adminClient = adminClient();
        Short rs=1; //副本因子 short类型
        NewTopic newTopic = new NewTopic(TOPIC_NAME,1,rs);
        CreateTopicsResult topics = adminClient.createTopics(Arrays.asList(newTopic));
        System.out.println("CreateTopicsResult: "+topics);
    }
    /*
    获取topic列表
     */
    public static void topicList() throws Exception {
        AdminClient adminClient = adminClient();
        //是否查看internal选项
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);
        ListTopicsResult listTopicsResult = adminClient.listTopics(options);
        Set<String> names = listTopicsResult.names().get();
        Collection<TopicListing> topicListings = listTopicsResult.listings().get();
        names.stream().forEach(System.out::println);
        topicListings.stream().forEach((topicList)->{
            System.out.println(topicList);
        });
    }

    /*
    删除topic
     */
    public static void delTopics() throws Exception {
        AdminClient adminClient = adminClient();
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(TOPIC_NAME));
        deleteTopicsResult.all().get();
    }
}
