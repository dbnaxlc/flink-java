package com.xzq.flink.table;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * Flink SQL的编程模型
创建一个TableEnvironment:TableEnvironment是Table API和SQL集成的核心概念，它主要负责:
　　1、在内部目录中注册一个Table
　　2、注册一个外部目录
　　3、执行SQL查询
　　4、注册一个用户自定义函数(标量、表及聚合)
　　5、将DataStream或者DataSet转换成Table
　　6、持有ExecutionEnvironment或者StreamExecutionEnvironment的引用
一个Table总是会绑定到一个指定的TableEnvironment中，相同的查询不同的TableEnvironment是无法通过join、union合并在一起。
TableEnvironment有一个在内部通过表名组织起来的表目录，Table API或者SQL查询可以访问注册在目录中的表，并通过名称来引用它们。

TableEnvironment允许通过各种源来注册一个表:
	1、一个已存在的Table对象，通常是Table API或者SQL查询的结果
		Table table = tableEnv.scan("X").select(…);
	2、TableSource，可以访问外部数据如文件、数据库或者消息系统
		TableSource csvSource = new CsvTableSource("/path/file", …);
	3、程序中的DataStream或者DataSet
		Table table= tableEnv.fromDataSet(tableset);
注册TableSink,可用于将 Table API或SQL查询的结果发送到外部存储系统，例如数据库，键值存储，消息队列或文件系统（例如，CSV）
	TableSink csvSink = new CsvTableSink("/path/file", ...); 
　　String[] fieldNames = {"a", "b", "c"}; 
    TypeInformation[] fieldTypes = {Types.INT, Types.STRING, Types.LONG}; 
    tableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, csvSink);
    
 * @author dbnaxlc
 * @date 2019年5月24日 上午9:05:17
 */
public class FlinkSql {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		List<WordCount> list  =  new ArrayList<WordCount>();
        String wordsStr = "Hello Flink Hello TOM";
        String[] words = wordsStr.split("\\W+");
        for(String word : words){
            WordCount wc = new WordCount(word, 1);
            list.add(wc);
        }
        DataSet<WordCount> input = env.fromCollection(list);
        tableEnv.registerDataSet("WordCount", input, "word, frequency");
        Table table = tableEnv.sqlQuery(
                "SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word");
        DataSet<WordCount> result = tableEnv.toDataSet(table, WordCount.class);
        result.print();
	}

}
