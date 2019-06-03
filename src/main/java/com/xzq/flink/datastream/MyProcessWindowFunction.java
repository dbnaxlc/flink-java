package com.xzq.flink.datastream;

import java.util.stream.StreamSupport;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * 一次性迭代整个窗口里的所有元素
 * @author dbnaxlc
 * @date 2019年6月3日 下午5:40:58
 */
public class MyProcessWindowFunction extends
		ProcessWindowFunction<Tuple2<String, Integer>, Tuple6<String, Integer, Integer, Integer, Integer, Integer>, String, GlobalWindow> {

	private static final long serialVersionUID = 7721222958777094759L;

	@Override
	public void process(String key,
			ProcessWindowFunction<Tuple2<String, Integer>, Tuple6<String, Integer, Integer, Integer, Integer, Integer>, String, GlobalWindow>.Context context,
			Iterable<Tuple2<String, Integer>> in,
			Collector<Tuple6<String, Integer, Integer, Integer, Integer, Integer>> out) throws Exception {
		Integer max = StreamSupport.stream(in.spliterator(), false).mapToInt(i -> i.f1).max().getAsInt();
		Integer min = StreamSupport.stream(in.spliterator(), false).mapToInt(i -> i.f1).min().getAsInt();
		Integer sum = StreamSupport.stream(in.spliterator(), false).mapToInt(i -> i.f1).sum();
		Integer count = Long.valueOf(StreamSupport.stream(in.spliterator(), false).mapToInt(i -> i.f1).count()).intValue();
		Integer avg = sum / count;
		out.collect(new Tuple6<String, Integer, Integer, Integer, Integer, Integer>(key, max, min, sum, count, avg));
		
	}

}
