package com.xzq.flink.dataset.anno;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 语义注解
 * @author dbnaxlc
 * @date 2019年6月6日 上午10:50:24
 */
public class DataSetSemanticAnnotations {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSource<Tuple2<String, Integer>> ds = env.fromElements(new Tuple2<String, Integer>("zhangsan", 1),
				new Tuple2<String, Integer>("wahah", 2), new Tuple2<String, Integer>("lisi", 4));
		ds.map(new MyForwardFields()).print();
		ds.map(new MyNonForwardFields()).print();
		ds.map(new MyReadFields()).print();
	}

}
