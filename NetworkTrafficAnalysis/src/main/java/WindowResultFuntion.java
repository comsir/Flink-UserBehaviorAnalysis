import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowResultFuntion implements WindowFunction<Long, UrlViewCount, String, TimeWindow> {
    @Override
    public void apply(String key, TimeWindow window, Iterable<Long> input, Collector<UrlViewCount> out) throws Exception {
//        String url = ((Tuple1<String>)tuple).f0;
        String url = key;
        Long count = input.iterator().next();
        out.collect(new UrlViewCount(url, window.getEnd(), count));
    }
}
