import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.awt.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LoginFailWithCep {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //source
        ArrayList<LoginEvent> list = new ArrayList<LoginEvent>();
        list.add(new LoginEvent(1L, "192.168.0.1", "fail", 1558430832L));
        list.add(new LoginEvent(1L, "192.168.0.2", "fail", 1558430843L));
        list.add(new LoginEvent(1L, "192.168.0.3", "fail", 1558430844L));
        list.add(new LoginEvent(1L, "192.168.0.3", "fail", 1558430845L));
        list.add(new LoginEvent(2L, "192.168.10.10", "success", 1558430845L));
        DataStream source = env.fromCollection(list);

        //定义一个匹配模式，next紧邻发生
        //begin：从什么事件开始
        //where：给个条件 什么样的情况下begin====>eventType == "fail"这个条件下 开始匹配
        //within : 一段时间内发生
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("begin").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return value.getEventType().equals("fail");
            }
        }).next("next").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return value.getEventType().equals("fail");
            }
        }).within(Time.seconds(2));

        KeyedStream keyedStream = source.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LoginEvent>() {
            @Override
            public long extractAscendingTimestamp(LoginEvent element) {
                return element.getEventTime() * 1000L;
            }
        })
                .keyBy(new KeySelector<LoginEvent, Long>() {
                    @Override
                    public Long getKey(LoginEvent value) throws Exception {
                        return value.getUserId();
                    }
                });

        //将keyedStream转化为PatternStream
        PatternStream patternStream = CEP.pattern(keyedStream, pattern);

        //从patternStream中提取到 模式匹配到的 事件流
patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
    StringBuilder sb = new StringBuilder();
    @Override
    public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
        LoginEvent next = pattern.getOrDefault("next", null).iterator().next();
        sb.append(next.getUserId()).append("+").append(next.getIp()).append("+").append(next.getEventType());
        return sb.toString();
    }
}).print();

env.execute("flink cep job");
    }


}
