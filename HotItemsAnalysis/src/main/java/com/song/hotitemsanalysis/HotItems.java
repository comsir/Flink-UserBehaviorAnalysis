package com.song.hotitemsanalysis;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class HotItems {
    final static private String Path = "D:\\Users\\15732\\IdeaProjects\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv";
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //2.读取数据
        DataStream<String> dataStream = env.readTextFile(Path);
        //3.操作数据
        DataStream<UserBehavior> data = dataStream.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String s) throws Exception {
                UserBehavior userBehavior = new UserBehavior();
                //561558,3611281,965809,pv,1511658000
                if (s.length() > 0) {
                    String[] split = s.split(",");
                    userBehavior.setUserId(Long.valueOf(split[0]));
                    userBehavior.setItemId(Long.valueOf(split[1]));
                    userBehavior.setCategoryId(Integer.valueOf(split[2]));
                    userBehavior.setBehavior(split[3]);
                    userBehavior.setTimestamp(Long.valueOf(split[4]));
                }
                return userBehavior;
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior userBehavior) {
                        return userBehavior.getTimestamp() * 1000L;
                    }
                });

        //做过滤
        DataStream<ItemViewCount> aggregate = data.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                return userBehavior.getBehavior().equals("pv");
            }
        }).keyBy("itemId").timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new CountAgg(), new WindowResult());//每个商品在每个窗口里点击量的数据流

        aggregate.keyBy("windowEnd") //再把相同时间窗口的商品归到一类
                .process(new TopNHotItems(3))
                .print();

        env.execute("hot items job");
    }
}
