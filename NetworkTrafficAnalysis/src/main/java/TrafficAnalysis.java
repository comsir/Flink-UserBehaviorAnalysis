import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.SimpleDateFormat;


//83.149.9.216 - - 17/05/2015:10:05:34 +0000 GET /presentations/logstash-monitorama-2013/images/sad-medic.png
public class TrafficAnalysis {
    final static private String Path = "D:\\Users\\15732\\IdeaProjects\\UserBehaviorAnalysis\\NetworkTrafficAnalysis\\src\\main\\resources\\apachetest.log";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> source = env.readTextFile(Path);
        source.map(new MapFunction<String, ApacheLogEvent>() {
            @Override
            public ApacheLogEvent map(String value) throws Exception {
                String[] split = value.split(" ");
                ApacheLogEvent apacheLogEvent = new ApacheLogEvent();
                //translate timestamp 转化时间格式
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                Long timestamp = simpleDateFormat.parse(split[3]).getTime();
                apacheLogEvent.setIp(split[0]);
                apacheLogEvent.setUserId(split[2]);
                apacheLogEvent.setEventTime(timestamp);
                apacheLogEvent.setMethod(split[5]);
                apacheLogEvent.setUrl(split[6]);
                return apacheLogEvent;
            }
        })
                //设置waterMark 乱序
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.milliseconds(5000)) {
            @Override
            public long extractTimestamp(ApacheLogEvent element) {
                return element.getEventTime();
            }
        })
                //keyby分流
        .keyBy(new KeySelector<ApacheLogEvent, String>() {
            @Override
            public String getKey(ApacheLogEvent value) {
                return value.getUrl();
            }
        })
                //滑动窗口设置
        .timeWindow(Time.minutes(10), Time.seconds(5))
                //窗口内聚合url
        .aggregate(new CountAgg(), new WindowResultFuntion())
                //keyb二次分流 根据时间窗口
        .keyBy(new KeySelector<UrlViewCount, Long>() {
            @Override
            public Long getKey(UrlViewCount value) throws Exception {
                return value.getWindowEnd();
            }
        })
                //聚合窗口 输出
        .process(new TopNHotUrls(5))
        .print();

        env.execute("traffic analysis job");

    }
}
