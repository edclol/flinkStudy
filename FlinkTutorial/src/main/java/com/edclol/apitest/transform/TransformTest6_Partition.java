package com.edclol.apitest.transform;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTutorial
 * Package: com.atguigu.apitest.transform
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/11/9 10:14
 */

import com.edclol.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URL;

/**
 * @ClassName: TransformTest6_Partition
 * @Description:
 * @Author: wushengran on 2020/11/9 10:14
 * @Version: 1.0
 */
public class TransformTest6_Partition {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 从文件读取数据
        URL resource = TransformTest5_RichFunction.class.getResource("/sensor.txt");
        DataStream<String> inputStream = env.readTextFile(resource.toString());

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.print("input");

        // 1. shuffle
        DataStream<String> shuffleStream = inputStream.shuffle();

//        shuffleStream.print("shuffle");

        // 2. keyBy

//        dataStream.keyBy("id").print("keyBy");

        // 3. global
        dataStream.global().print("global");

        env.execute();
    }
}
