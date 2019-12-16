import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import java.util.ArrayList;

public class LoginFail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //source
        ArrayList<LoginEvent> list = new ArrayList<LoginEvent>();
        list.add(new LoginEvent(1L, "192.168.0.1", "fail", 1558430842L));
        list.add(new LoginEvent(1L, "192.168.0.2", "fail", 1558430843L));
        list.add(new LoginEvent(1L, "192.168.0.3", "fail", 1558430844L));
        list.add(new LoginEvent(2L, "192.168.10.10", "success", 1558430845L));
        DataStream source = env.fromCollection(list);

        source.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LoginEvent>() {
            @Override
            public long extractAscendingTimestamp(LoginEvent element) {
                return element.getEventTime() * 1000;
            }
        })
                .keyBy(new KeySelector<LoginEvent, Long>() {
                    @Override
                    public Long getKey(LoginEvent value) throws Exception {
                        return value.getUserId();
                    }
                })
                .process(new MatchFuntion())
                .print();
        env.execute("Login Fail Detect Job");

    }

}
