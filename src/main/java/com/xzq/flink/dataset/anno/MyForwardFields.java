package com.xzq.flink.dataset.anno;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple2;

@ForwardedFields({"f0->f1","f1->f0"})
public class MyForwardFields implements MapFunction<Tuple2<String, Integer>, Tuple2<Integer, String>> {

	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<Integer, String> map(Tuple2<String, Integer> value) throws Exception {
		return new Tuple2<Integer, String>(value.f1, value.f0);
	}

}
