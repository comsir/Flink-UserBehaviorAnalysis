import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;

public class TopNHotUrls extends KeyedProcessFunction<Long, UrlViewCount, String> {

    private int topSize;
    public TopNHotUrls(int topSize) {
        this.topSize = topSize;
    }

    //状态操作
    private ListState<UrlViewCount> listState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //listState关联上下文
        ListStateDescriptor<UrlViewCount> listStateDesc = new ListStateDescriptor<UrlViewCount>("urlState-state", UrlViewCount.class);
        listState = getRuntimeContext().getListState(listStateDesc);
    }

    @Override
    public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
        //将一个窗口内的数据装在listState中
        listState.add(value);
        //注册触发器
        //当waterMark到本窗口结束的下一秒 出发这个窗口的计算 也就是出发 onTimer（）
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 5 * 1000);

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        //一旦出发了这个onTimer触发器 从listState中拿出所有数据 处理完 然后清空listState
        ArrayList<UrlViewCount> list = new ArrayList<UrlViewCount>();
        StringBuilder result = new StringBuilder();
        for (UrlViewCount state:
                listState.get()) {
            list.add(state);
        }
        //清空这个窗口的state
        listState.clear();
        //然后就是对这个arraylist输出排序 前topN
        Collections.sort(list, new Comparator<UrlViewCount>() {
            @Override
            public int compare(UrlViewCount o1, UrlViewCount o2) {
                if(o1.getCount() == o2.getCount()) {
                    return 0;
                }else {
                    if(o1.getCount() < o2.getCount()) {
                        return 1;
                    }else {
                        return -1;
                    }
                }
            }
        });

        result.append("====================\n");
        result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n");
        for (int j = 0; j < topSize; j++) {
            UrlViewCount cur = list.get(j);
            // e.g.  No1：  商品ID=12224  浏览量=2413
            result.append("No").append(j + 1).append(":")
                    .append("  Url=").append(cur.getUrl())
                    .append("  流量=").append(cur.getCount()).append("\n");
        }

        result.append("=====================\n\n");
        Thread.sleep(1000);
        out.collect(result.toString());
    }
}
