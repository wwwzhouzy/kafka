package com.zhouzy.kafka;


import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Before;
import org.junit.Test;

/**
 * 注：jdk1.8.0_113
 * @author Administrator
 *
 */
public class ConsumeTest {
    private Properties props;

    @Before
    public void init() {
        props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
      //消费者的组id
        props.put("group.id", "testConsumer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
      //从poll(拉)的回话处理时长
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    @Test
    public void consume() {
        System.out.println("begin consumer");
        connectionKafka();
        System.out.println("finish consumer");
    }

    @SuppressWarnings("resource")
    public void connectionKafka() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
      //订阅主题列表topic
        consumer.subscribe(Arrays.asList("test"));

        while (true) {
        	//poll的数量限制
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {
                System.out.println("收到消息：" + record.value());
            }
        }
    }
}
