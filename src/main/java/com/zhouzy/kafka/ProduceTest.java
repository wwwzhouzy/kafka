package com.zhouzy.kafka;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ProduceTest {
    private static Scanner scanner;

    private Producer<String, String> producer;
    private Properties props;

    @Before
    public void init() {
        props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
      //“所有”设置将导致记录的完整提交阻塞，最慢的，但最持久的设置。
        props.put("acks", "all");
      //如果请求失败，生产者也会自动重试，即使设置成０
        props.put("retries", 0);
      //The producer maintains buffers of unsent records for each partition
        props.put("batch.size", 16384);
        //默认立即发送，这里这是延时毫秒数
        props.put("linger.ms", 1);
        //生产者缓冲大小，当缓冲区耗尽后，额外的发送调用将被阻塞。时间超过max.block.ms将抛出TimeoutException
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    @Test
    public void produce() {
        System.out.println("begin produce");
        connectionKafka();
        System.out.println("finish produce");
    }

    public void connectionKafka() {
    	//创建kafka的生产者类
        producer = new KafkaProducer<>(props);
        scanner = new Scanner(System.in);

        while (true) {
            System.out.println("请输入要发送的消息：");
            String value = scanner.nextLine();
            if (value.equals("exit")) {
                break;
            }

            producer.send(new ProducerRecord<String, String>("test", value));
        }

    }

    @After
    public void destroy() {
        producer.close();
    }

}
