package com.xzq.flink.table.tablesource;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.BatchTableSource;

import com.xzq.flink.table.dto.WordCount;

public class MyTableSource implements BatchTableSource<WordCount> {

	/**
	 * 返回描述TableSource的字符串 。此方法是可选的，仅用于显示目的
	 */
	@Override
	public String explainSource() {
		StringBuilder builder = new StringBuilder();
		builder.append("WordCount [word : ");
		builder.append(String.class.getName());
		builder.append(", frequency=");
		builder.append(Long.class.getName());
		builder.append("]");
		return builder.toString();
	}

	/**
	 * 返回DataSet（BatchTableSource）的物理类型以及由此生成的记录TableSource
	 */
	@Override
	public TypeInformation<WordCount> getReturnType() {
		return Types.POJO(WordCount.class);
	}

	/**
	 * 返回表的结构，即表的字段的名称和类型。字段类型使用Flink定义TypeInformation
	 */
	@Override
	public TableSchema getTableSchema() {
		String[] fieldNames = {"word", "frequency"};
		TypeInformation[] fieldTypes = {Types.STRING, Types.LONG};
		return new TableSchema(fieldNames, fieldTypes);
	}

	/**
	 * 返回包含表数据的DataSet。DataSet的类型必须与TableSource.getReturnType()方法定义的返回类型相同
	 */
	@Override
	public DataSet<WordCount> getDataSet(ExecutionEnvironment env) {
		String wordsStr = "Hello Flink Hello TOM";
        String[] words = wordsStr.split("\\W+");
        List<WordCount> list  =  new ArrayList<WordCount>();
        for(String word : words){
            WordCount wc = new WordCount(word, 1);
            list.add(wc);
        }
        return env.fromCollection(list);
	}

}
