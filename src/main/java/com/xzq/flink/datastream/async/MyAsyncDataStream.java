package com.xzq.flink.datastream.async;

import java.util.concurrent.TimeUnit;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyAsyncDataStream {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Integer> ds = env.fromElements(1001,2002,3003,4004);
//		DataStream<String> result = AsyncDataStream.unorderedWait(ds, new MyAsyncFunction(), 2l, TimeUnit.SECONDS, 15).setParallelism(1);
//		result.print();
		DataStream<String> result = AsyncDataStream.orderedWait(ds, new MyAsyncFunction(), 2l, TimeUnit.SECONDS, 15).setParallelism(1);
		result.print();
		env.execute("order");
		
	}

}
