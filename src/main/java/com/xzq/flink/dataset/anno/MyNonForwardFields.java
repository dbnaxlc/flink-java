package com.xzq.flink.dataset.anno;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation.NonForwardedFields;
import org.apache.flink.api.java.tuple.Tuple2;

@NonForwardedFields("f1")
public class MyNonForwardFields implements MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
		return new Tuple2<String, Integer>(value.f0, value.f1*2);
	}

}
