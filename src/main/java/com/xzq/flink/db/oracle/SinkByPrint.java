package com.xzq.flink.db.oracle;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class SinkByPrint extends RichSinkFunction<CapitalAccount> {

	private static final long serialVersionUID = -6708645094770191907L;

	@Override
	public void invoke(CapitalAccount value) throws Exception {
		System.out.println(Thread.currentThread() + "---" + value);
	}

	
}
