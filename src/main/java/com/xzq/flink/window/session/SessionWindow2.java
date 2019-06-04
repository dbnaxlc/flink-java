package com.xzq.flink.window.session;

import java.text.SimpleDateFormat;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * 会话窗口，采用会话持续时长作为窗口处理依据。设置指定的会话持续时长时间，在这段时间中不再出现会话则认为超出会话时长。
 * @author dbnaxlc
 * @date 2019年5月22日 上午10:21:10
 */
public class SessionWindow2 {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		long delay = 5000L;
        long windowGap = 10000L;
        final OutputTag<Tuple3<String, String, Long>> REJECTEDWORDSTAG = new OutputTag<Tuple3<String, String, Long>>("rejected_words_tag"){};

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        // 设置数据源
        DataStream<Tuple3<String, String, Long>> source = env.addSource(new StreamDataSource()).name("Demo Source");
        // 设置水位线
        DataStream<Tuple3<String, String, Long>> stream = source.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.milliseconds(delay)) {
                @Override
                public long extractTimestamp(Tuple3<String, String, Long> element) {
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    System.out.println(element.f0 + "\t" + element.f1 + " watermark -> " + format.format(getCurrentWatermark().getTimestamp()) + " eventTime -> " + format.format(element.f2));
                    return element.f2;
                }
            }
        );
        // 窗口聚合,event_time + windowGap < watermark时，数据丢失
        SingleOutputStreamOperator<Tuple3<String, String, Long>> sides = stream.keyBy(0).window(EventTimeSessionWindows.withGap(Time.milliseconds(windowGap)))
        		.allowedLateness(Time.seconds(1))
        //超过watermark的数据，侧路输出
        .sideOutputLateData(REJECTEDWORDSTAG)
        .reduce(
            new ReduceFunction<Tuple3<String, String, Long>>() {
                @Override
                public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1, Tuple3<String, String, Long> value2) throws Exception {
                    return Tuple3.of(value1.f0, value1.f1 + "-" + value2.f1, 1L);
                }
            }
        );
        sides.print();
        // 记录时间乱序事件
        DataStream<String> events =
            sides.getSideOutput(REJECTEDWORDSTAG)
                 .map(new MapFunction<Tuple3<String, String, Long>, String>() {
                     @Override public String map(Tuple3<String, String, Long> event) throws Exception {
                         return "rejected_"+event;
                     }
                 });
        events.print();
        
        env.execute("TimeWindowDemo");
	}

}
