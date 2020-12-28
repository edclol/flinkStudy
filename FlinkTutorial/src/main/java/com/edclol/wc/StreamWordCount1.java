package com.edclol.wc;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTutorial
 * Package: com.atguigu.wc
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/11/6 11:48
 */

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: StreamWordCount
 * @Description:
 * @Author: wushengran on 2020/11/6 11:48
 * @Version: 1.0
 */
public class StreamWordCount1 {
    public static void main(String[] args) throws Exception{
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从socket文本流读取数据
        DataStream<String> inputDataStream = env.socketTextStream("localhost", 7777);

        inputDataStream.print();

        // 执行任务
        env.execute();
    }
}
