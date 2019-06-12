package com.xzq.flink.table.function;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkTableFunctionTest {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        DataSet<String> ds = env.fromElements("wa#ha", "wa#ha12", "wa#ha3455");
        tableEnv.registerDataSet("str", ds);
        tableEnv.registerFunction("split", new SplitTableFunction("#"));
        Table table = tableEnv.sqlQuery("SELECT s.f0, word, length FROM str s, LATERAL TABLE(split(f0)) as T(word, length)");
        DataSet<Row> result = tableEnv.toDataSet(table, Row.class);
        result.print();
        Table table1 = tableEnv.sqlQuery("SELECT f0, word, length FROM str LEFT JOIN LATERAL TABLE(split(f0)) as T(word, length) ON TRUE");
        DataSet<Row> result1 = tableEnv.toDataSet(table1, Row.class);
        result1.print();
        
        Table table2 = tableEnv.scan("str");
        Table table3 = table2.join(new Table(tableEnv, "split(f0) as (word, length)")).select("f0, word, length");
        DataSet<Row> result2 = tableEnv.toDataSet(table3, Row.class);
        result2.print();
        
        Table table4 = table2.leftOuterJoin(new Table(tableEnv, "split(f0) as (word, length)")).select("f0, word, length");
        DataSet<Row> result3 = tableEnv.toDataSet(table4, Row.class);
        result3.print();
	}

}
