package com.xzq.flink.tuple;

import org.apache.flink.api.java.tuple.Tuple2;

public class SimpleTuple extends Tuple2<String, Integer> {

	private static final long serialVersionUID = -1599244045506577613L;
	
	public SimpleTuple() {
		super();
	}

	public SimpleTuple(String f0, Integer f1) {
		this.f0 = f0;
		this.f1 = f1;
	}
	
	
}
