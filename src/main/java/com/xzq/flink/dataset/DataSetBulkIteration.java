package com.xzq.flink.dataset;

import java.util.Random;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

public class DataSetBulkIteration {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		//初始迭代数据集
		IterativeDataSet<Integer> ds = env.fromElements(0).iterate(100);
		//map是step function
		ds.closeWith(ds.map(i -> i+ new Random().nextInt(10))).print();

	}

}
