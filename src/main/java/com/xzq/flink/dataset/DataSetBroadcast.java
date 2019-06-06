package com.xzq.flink.dataset;

import java.util.List;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

public class DataSetBroadcast {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Integer> toBroadcast = env.fromElements(1,2,3);
		DataSet<String> data = env.fromElements("a", "b");
		data.map(new RichMapFunction<String, String>() {

			private static final long serialVersionUID = 1430289052340897599L;
			
			private List<Integer> broad = null;

			@Override
			public void open(Configuration config) throws Exception {
				super.open(config);
				broad = getRuntimeContext().getBroadcastVariable("broad");
			}

			@Override
			public String map(String value) throws Exception {
				return value + broad.size();
			}
		}).withBroadcastSet(toBroadcast, "broad").print();

	}

}
