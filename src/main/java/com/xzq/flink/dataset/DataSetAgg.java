package com.xzq.flink.dataset;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

public class DataSetAgg {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSource<Integer> ds = env.fromElements(1,2,3,4,5,6,1);
		ds.reduce(Integer::sum).print();
		DataSource<Tuple2<Integer, Integer>> tuples = env.fromElements(new Tuple2<Integer, Integer>(1,101), 
				new Tuple2<Integer, Integer>(2,312),
				new Tuple2<Integer, Integer>(4,1));
		tuples.aggregate(Aggregations.SUM, 0).print();
		tuples.aggregate(Aggregations.MIN, 1).print();
		ds.distinct().print();
	}

}
