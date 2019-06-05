package com.xzq.flink.dataset;

import java.util.Iterator;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class DateSetFromInput {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSource<Row> ds = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername("com.mysql.jdbc.Driver")
				.setDBUrl("jdbc:mysql://172.16.5.115:3306/admin?useUnicode=true&characterEncoding=utf-8&useSSL=false")
				.setUsername("root").setPassword("").setQuery("select user_id, username from sys_user")
				.setRowTypeInfo(new RowTypeInfo(TypeInformation.of(Integer.class), TypeInformation.of(String.class)))
				.finish());
		ds.map(value -> new Tuple2<Integer, String>((Integer) value.getField(0), (String) value.getField(1)))
				.returns(new TypeHint<Tuple2<Integer, String>>() {})
				.print();
		ds.mapPartition(new MapPartitionFunction<Row, Tuple2<Integer, String>>() {

			private static final long serialVersionUID = -8382277236141306288L;

			@Override
			public void mapPartition(Iterable<Row> values, Collector<Tuple2<Integer, String>> out)
					throws Exception {
				Iterator<Row> vs = values.iterator();
				while(vs.hasNext()) {
					Row value = vs.next();
					out.collect(new Tuple2<Integer, String>((Integer) value.getField(0), (String) value.getField(1)));
				}
			}
		}).print();;

	}

}
