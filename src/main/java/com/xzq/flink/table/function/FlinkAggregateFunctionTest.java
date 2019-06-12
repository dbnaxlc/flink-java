package com.xzq.flink.table.function;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import com.xzq.flink.table.dto.SumAvg;

public class FlinkAggregateFunctionTest {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        DataSet<SumAvg> ds = env.fromElements(new SumAvg(1,1),
        		new SumAvg(2,2), new SumAvg(3,3));
        tableEnv.registerDataSet("str", ds);
        tableEnv.registerFunction("sum_avg", new SumAvgAggregateFunction());
        Table table = tableEnv.sqlQuery("SELECT sum_avg(number1, number2) FROM str");
        DataSet<Row> result = tableEnv.toDataSet(table, Row.class);
        result.print();
	}

}
