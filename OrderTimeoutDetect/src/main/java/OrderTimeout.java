import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class OrderTimeout {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        KeyedStream<OrderEvent, Long> orderEventStream  = env.fromCollection(Arrays.asList(
                new OrderEvent(1L, "create", 1558430842L),
                new OrderEvent(2L, "create", 1558430843L),
                new OrderEvent(2L, "pay", 1558430844L)
        )).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
            @Override
            public long extractAscendingTimestamp(OrderEvent element) {
                return element.getEventTime() * 1000;
            }
        })
                .keyBy(new KeySelector<OrderEvent, Long>() {
                    @Override
                    public Long getKey(OrderEvent value) throws Exception {
                        return value.getOrderId();
                    }
                });

        //定义pattern
        Pattern<OrderEvent, OrderEvent> OrderPayPattern = Pattern.<OrderEvent>begin("begin").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return value.getEventType().equals("create");
            }
        }).followedBy("follow").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return value.getEventType().equals("pay");
            }
        }).within(Time.seconds(15));

        //定义超时支付的输出标签 用于标签 side输出
        OutputTag<OrderResult> orderTimeoutOutput = new OutputTag<OrderResult>("orderTimeout"){};
       // OutputTag<Tuple2<String, Long>> info = new OutputTag<Tuple2<String, Long>>("late-data"){};

        //将keyby后的流绑定模式 keybyStream===>patternStream
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventStream, OrderPayPattern);
        //从patternStream中获取匹配的流（包括超时的和正常的  等等要分别处理）
        DataStream<OrderResult> completedDataStream = patternStream.select(orderTimeoutOutput, new PatternTimeoutFunction<OrderEvent, OrderResult>() {
            @Override
            public OrderResult timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
                Long timeoutOrderId = pattern.getOrDefault("begin", null).iterator().next().getOrderId();
                return new OrderResult(timeoutOrderId, "timeout");
            }
        }, new PatternSelectFunction<OrderEvent, OrderResult>() {
            @Override
            public OrderResult select(Map<String, List<OrderEvent>> pattern) throws Exception {
                Long payOrderId = pattern.getOrDefault("follow", null).iterator().next().getOrderId();
                return new OrderResult(payOrderId, "success");
            }
        });

        completedDataStream.print();
        DataStream<OrderResult> sideOutput = ((SingleOutputStreamOperator<OrderResult>) completedDataStream).getSideOutput(orderTimeoutOutput);
        sideOutput.print();

        env.execute("order timeout job");

    }
}
