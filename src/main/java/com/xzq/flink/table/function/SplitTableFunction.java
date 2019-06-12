package com.xzq.flink.table.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableFunction;

/**
 * 对单个或者多个输入字段计算后返回一个或者多个列，类似table
 * @author dbnalxc
 * @date 2019年6月12日 上午11:42:00
 */
public class SplitTableFunction extends TableFunction<Tuple2<String, Integer>> {
	private static final long serialVersionUID = -8188608617886463486L;
	private String separator = " ";

	public SplitTableFunction(String separator) {
		this.separator = separator;
	}

	public void eval(String str) {
		for (String s : str.split(separator)) {
			collect(new Tuple2<String, Integer>(s, s.length()));
		}
	}
}
