import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class MatchFuntion extends KeyedProcessFunction<Long, LoginEvent, LoginEvent > {

    private ListState<LoginEvent> loginState;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //状态编程
        loginState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("LoginState", LoginEvent.class));

    }


    @Override
    public void processElement(LoginEvent value, Context ctx, Collector<LoginEvent> out) throws Exception {
       if(value.getEventType().equals("fail")) {
           loginState.add(value);
       }
       //注册定时器
        ctx.timerService().registerEventTimeTimer(value.getEventTime() + 2 * 1000);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginEvent> out) throws Exception {
        ArrayList<LoginEvent> list = new ArrayList<LoginEvent>();
        for (LoginEvent event :
                loginState.get()) {
            list.add(event);
        }
        loginState.clear();
        if(list.size() > 1) {
            out.collect(list.get(0));
        }
    }
}
