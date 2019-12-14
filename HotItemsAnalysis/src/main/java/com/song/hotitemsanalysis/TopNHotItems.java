package com.song.hotitemsanalysis;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.*;

public class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {
    
    private int topSize;
    private ListState<ItemViewCount> itemState;

    public TopNHotItems(int topSize) {
        this.topSize = topSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //命名状态变量的名字和类型
        ListStateDescriptor<ItemViewCount> itemStateDesc = new ListStateDescriptor<ItemViewCount>("itemState", ItemViewCount.class);
        itemState = getRuntimeContext().getListState(itemStateDesc);
    }

    @Override
    public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
        //每个一个数据过来的时候存到我们的状态里
        itemState.add(value);
        //根据windowEnd然后注册一个定时器
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        //窗口关闭的时候 出发 数据做排序输出
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        //获取所有商品点击信息
        List<ItemViewCount> list = new ArrayList<ItemViewCount>();
        StringBuilder result = new StringBuilder();
        Iterable<ItemViewCount> itemlist = itemState.get();
        for (ItemViewCount item :
                itemlist) {
            list.add(item);
        }
        //清除状态中的数据 释放空间
        itemState.clear();
        //已经拿到所有数据 现在排序
        Collections.sort(list,new SortByCount());
        //排序数据格式化 方便打印输出
        result.append("====================\n");
        result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n");
        for (int j = 0; j < topSize; j++) {
            ItemViewCount cur = list.get(j);
            // e.g.  No1：  商品ID=12224  浏览量=2413
            result.append("No").append(j + 1).append(":")
                    .append("  商品ID=").append(cur.getItemId())
                    .append("  浏览量=").append(cur.getCount()).append("\n");
        }

        result.append("=====================\n\n");
        Thread.sleep(1000);
        out.collect(result.toString());
    }
    
}
