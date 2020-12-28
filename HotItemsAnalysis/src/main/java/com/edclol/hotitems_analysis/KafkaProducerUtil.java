package com.edclol.hotitems_analysis;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: UserBehaviorAnalysis
 * Package: com.atguigu.hotitems_analysis
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/11/14 16:25
 */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

/**
 * @ClassName: KafkaProducerUtil
 * @Description:
 * @Author: wushengran on 2020/11/14 16:25
 * @Version: 1.0
 */
public class KafkaProducerUtil {
    public static void main(String[] args) throws Exception {
        writeToKafka("hotitems", "/UserBehavior.csv");
    }

    // 包装一个写入kafka的方法
    public static void writeToKafka(String topic,String path) throws Exception{
        // kafka 配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop200:9092,hadoop201:9092,hadoop202:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 定义一个Kafka Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        String s = KafkaProducerUtil.class.getResource(path).toString();
        // 用缓冲方式读取文本
        BufferedReader bufferedReader = new BufferedReader(new FileReader(s.replaceFirst("file:/","")));
        String line;
        long count = 0L;
        while ((line = bufferedReader.readLine()) != null) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, line);
            // 用producer发送数据
            kafkaProducer.send(producerRecord);
            count += 1;
        }
        System.out.println("向Topic: "+topic+" 写入了 "+count+" 条数据");
        kafkaProducer.close();
    }
}
