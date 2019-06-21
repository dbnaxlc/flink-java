package com.xzq.flink.table.function;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 
 * @author dbnaxlc
 * @date 2019年6月12日 上午11:15:14
 */
public class FlinkScalarFunctionTest {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Map<String, String> map = new HashMap<>();
		map.put("db.url", "10.37.10.100");
		env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(map));
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        DataSet<Tuple2<Integer, Integer>> input = env.fromElements(new Tuple2<Integer, Integer>(1,1),
        		new Tuple2<Integer, Integer>(2,2),
        		new Tuple2<Integer, Integer>(3,3));
        tableEnv.registerDataSet("addTest", input);
        tableEnv.registerFunction("add", new AddScalarFunction());
        Table table = tableEnv.sqlQuery(
                "SELECT f0, f1, add(f0, f1) as total FROM addTest");
        DataSet<Row> result = tableEnv.toDataSet(table, Row.class);
        result.print();
        DataSet<Tuple2<Integer, Long>> ls = env.fromElements(new Tuple2<Integer, Long>(1,System.currentTimeMillis()));
        tableEnv.registerDataSet("lsTest", ls);
        tableEnv.registerFunction("longToTimestamp", new LongToTimestamp());
        Table table1 = tableEnv.sqlQuery(
                "SELECT longToTimestamp(f1) from lsTest");
        DataSet<Row> result1 = tableEnv.toDataSet(table1, Row.class);
        result1.print();
	}

}
