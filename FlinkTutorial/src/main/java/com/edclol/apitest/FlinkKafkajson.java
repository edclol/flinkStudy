package com.edclol.apitest;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * Created by lhq on 2019/6/11.
 */
public class FlinkKafkajson {
    public static void main(String[] args) throws Exception {

        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        tableEnvironment.connect(new Kafka().version("0.8").topic("lhqtest").startFromLatest()
                .property("bootstrap.servers", "192.168.x.x:9092")
                .property("zookeeper.connect", "192.168.x.x:2181")
                .property("group.id", "lhqtest"))
                .withFormat(new Json().failOnMissingField(true))
                .withSchema(new Schema()
                        .field("id", Types.INT)
                        .field("name", Types.STRING)
                        .field("sex", Types.STRING)

                )
                .inAppendMode()
                .registerTableSource("lhq_user");
        Table table = tableEnvironment.scan("lhq_user").select("id,name,sex");
        DataStream<Row> personDataStream = tableEnvironment.toAppendStream(table, Row.class);
        personDataStream.addSink(new SinkFunction<Row>() {
            @Override
            public void invoke(Row value, Context context) throws Exception {

            }
        });
        env.execute("userPv from Kafka");

    }
}
