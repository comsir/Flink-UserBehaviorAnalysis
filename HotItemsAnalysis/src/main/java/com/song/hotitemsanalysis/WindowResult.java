package com.song.hotitemsanalysis;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


//输出格式：ItemViewCount
public class WindowResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
        Long itemId = ((Tuple1<Long>)tuple).f0;
        Long count = input.iterator().next();
        out.collect(new ItemViewCount(itemId, window.getEnd(), count));
    }
}
