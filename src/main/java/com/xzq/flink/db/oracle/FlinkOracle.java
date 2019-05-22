package com.xzq.flink.db.oracle;

import java.math.BigDecimal;
import java.util.Date;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlinkOracle {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.addSource(new SourceFromOracle())
				// 输入是一个数据流，输出的也是一个数据流
				.map(value -> {
					CapitalAccount ca = new CapitalAccount();
					ca.setCapitalAccountId(value.getCapitalAccountId());
					ca.setBalance(value.getBalance() + 2);
					ca.setCreatedTime(new Date());
					return ca;
				})
				//采用一条记录并输出零个，一个或多个记录
				.flatMap(new FlatMapFunction<CapitalAccount, CapitalAccount>() {
					public void flatMap(CapitalAccount value, Collector<CapitalAccount> out) throws Exception {
						if (value.getBalance().longValue() % 2 == 0) {
							out.collect(value);
						}
					}
				})
				//根据条件判断结果
				.filter(value -> value.getBalance().longValue() % 3 == 0)
				//KeyBy 在逻辑上是基于 key 对流进行分区。在内部，它使用 hash 函数对流进行分区。它返回 KeyedDataStream 数据流。
				.keyBy(value -> value.getCapitalAccountId())
				//Reduce 返回单个的结果值，并且 reduce 操作每处理一个元素总是创建一个新值。常用的方法有 average, sum, min, max, count，使用 reduce 方法都可实现
				.reduce((value1, value2) -> {
					CapitalAccount ca = new CapitalAccount();
					ca.setUseableAmt(new BigDecimal(value1.getBalance()));
					return ca;})
//				.print();
				.addSink(new SinkByPrint()).setParallelism(1);
		env.addSource(new SourceFromOracle())
		.flatMap(new FlatMapFunction<CapitalAccount, Tuple2<Long, Double>>() {

			@Override
			public void flatMap(CapitalAccount value, Collector<Tuple2<Long, Double>> out) throws Exception {
				out.collect(new Tuple2<>(value.getCapitalAccountId(), value.getBalance()));
			}
		})
		//KeyBy 在逻辑上是基于 key 对流进行分区。在内部，它使用 hash 函数对流进行分区。它返回 KeyedDataStream 数据流。
		.keyBy(0)
		.sum(1).print();
		env.execute("flink-oralce");
		
	}

}
