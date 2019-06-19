package com.xzq.flink.hotitem;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

	private static final long serialVersionUID = 8080944357899333593L;

	@SuppressWarnings("unchecked")
	@Override
	public void apply(Tuple key, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out)
			throws Exception {
		out.collect(ItemViewCount.of(((Tuple1<Long>) key).f0, window.getEnd(), input.iterator().next()));
	}


}
