package com.xzq.flink.dataset;

import java.util.Iterator;
import java.util.stream.StreamSupport;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class DataSetDeltaIteration {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple2<Long, Double>> initialSolutionSet = env.fromElements(new Tuple2<Long, Double>(1l, 0.1d));

		DataSet<Tuple2<Long, Double>> initialDeltaSet = env.fromElements(new Tuple2<Long, Double>(2l, 0.2d));
		int maxIterations = 100;
		int keyPosition = 0;

		DeltaIteration<Tuple2<Long, Double>, Tuple2<Long, Double>> iteration = initialSolutionSet
		    .iterateDelta(initialDeltaSet, maxIterations, keyPosition);

		DataSet<Tuple2<Long, Double>> candidateUpdates = iteration.getWorkset()
		    .groupBy(1)
		    .reduceGroup(new GroupReduceFunction<Tuple2<Long,Double>, Tuple2<Long, Double>>() {
		    	
				private static final long serialVersionUID = 6902184175963963170L;

				@Override
				public void reduce(Iterable<Tuple2<Long, Double>> values, Collector<Tuple2<Long, Double>> out)
						throws Exception {
					Iterator<Tuple2<Long, Double>> ite = values.iterator();
					while(ite.hasNext()) {
						Tuple2<Long, Double> item = ite.next();
						out.collect(new Tuple2<Long, Double>(item.f0, 2*item.f1));
					}
				}
			});

		DataSet<Tuple2<Long, Double>> deltas = candidateUpdates
		    .join(iteration.getSolutionSet())
		    .where(0)
		    .equalTo(0)
		    .with(new FlatJoinFunction<Tuple2<Long,Double>, Tuple2<Long,Double>, Tuple2<Long,Double>>() {

				private static final long serialVersionUID = 1L;

				@Override
				public void join(Tuple2<Long, Double> first, Tuple2<Long, Double> second, Collector<Tuple2<Long,Double>> out)
						throws Exception {
					if( second != null) {
						out.collect(new Tuple2<Long, Double>(first.f0, first.f1 + second.f1));
					} else {
						out.collect(first);
					}
					
				}
			});

		DataSet<Tuple2<Long, Double>> nextWorkset = deltas
		    .filter(i -> i.f0 <1);

		iteration.closeWith(deltas, nextWorkset).print();
	}

}
