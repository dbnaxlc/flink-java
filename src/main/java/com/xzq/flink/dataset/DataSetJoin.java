package com.xzq.flink.dataset;

import java.util.Iterator;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class DataSetJoin {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSource<Tuple2<Integer, Integer>> ds1 = env.fromElements(new Tuple2<Integer, Integer>(1, 101),
				new Tuple2<Integer, Integer>(2, 312), new Tuple2<Integer, Integer>(4, 1));
		DataSource<Tuple2<Integer, String>> ds2 = env.fromElements(new Tuple2<Integer, String>(1, "zhangsan"),
				new Tuple2<Integer, String>(2, "wahah"), new Tuple2<Integer, String>(4, "lisi"));
		ds1.join(ds2).where(0).equalTo(0).map(i -> new Tuple3<Integer, Integer, String>(i.f0.f0, i.f0.f1, i.f1.f1))
				.returns(new TypeHint<Tuple3<Integer, Integer, String>>() {
				}).print();

		ds1.coGroup(ds2).where(0).equalTo(0).with((Iterable<Tuple2<Integer, Integer>> first,
							Iterable<Tuple2<Integer, String>> second, Collector<Tuple3<Integer, Integer, String>> out)
						-> {
						Iterator<Tuple2<Integer, Integer>> value1 = first.iterator();
						Iterator<Tuple2<Integer, String>> value2 = second.iterator();
						while (value1.hasNext()) {
							Tuple2<Integer, Integer> v1 = value1.next();
							if (value2.hasNext()) {
								Tuple2<Integer, String> v2 = value2.next();
								out.collect(new Tuple3<Integer, Integer, String>(v1.f0, v1.f1, v2.f1));
							}
						}
					})
		.returns(new TypeHint<Tuple3<Integer, Integer, String>>() {}).print();
	}

}
