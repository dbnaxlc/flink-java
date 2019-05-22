package com.xzq.flink.window.count;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 滑动计数窗口
 * 
 * @author dbnaxlc
 * @date 2019年5月22日 上午10:10:10
 */
public class SlidingCountWindow {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.fromElements("wahah-1,wahah-2,wahah-3,wahah-4,wahah-5,wahah-6,wahah-7,wahah-8,wahah-9")
				.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
					public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
						String[] strs = value.split(",");
						for (String str : strs) {
							String[] mkv = str.split("-");
							out.collect(new Tuple2<String, Integer>(mkv[0], Integer.parseInt(mkv[1])));
						}
					}
				}).keyBy(0)
				// 滚动窗口，每1次事件计算最近10次总和
				.countWindow(10, 1).sum(1).print();
		env.execute("sliding-count-window");
	}

}
