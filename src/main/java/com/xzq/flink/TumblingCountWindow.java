package com.xzq.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 计数窗口，采用事件数量作为窗口处理依据。计数窗口分为滚动和滑动两类，使用keyedStream.countWindow实现计数窗口定义
 * @author dbnaxlc
 * @date 2019年5月22日 上午9:04:10
 */
public class TumblingCountWindow {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.fromElements("wahah-1,wahah-2,wahah-3,wahah-4,wahah-5,wahah-6,wahah-7,wahah-8,wahah-9")
			.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
				public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
					String[] strs = value.split(",");
					for(String str : strs) {
						String[] mkv = str.split("-");
						out.collect(new Tuple2<String, Integer>(mkv[0], Integer.parseInt(mkv[1])));
					}
				}
				}).keyBy(0).countWindow(3).sum(1).print();
		env.execute("count-window");
	}

}
