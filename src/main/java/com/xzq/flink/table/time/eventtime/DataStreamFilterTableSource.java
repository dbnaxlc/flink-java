package com.xzq.flink.table.time.eventtime;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.FilterableTableSource;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;
import org.apache.flink.types.Row;

/**
 * 添加事件时间处理列
 * 
 * @author XIAZHIQIANG
 *
 */
public class DataStreamFilterTableSource implements StreamTableSource<Row>, FilterableTableSource {

	@Override
	public String explainSource() {
		StringBuilder builder = new StringBuilder();
		builder.append("Trade [catg :");
		builder.append(String.class.getName());
		builder.append(", userId : int");
		builder.append(", ts : java.lang.Long");
		builder.append(", price : double");
		builder.append("]");
		return builder.toString();
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		String[] names = new String[] { "Username", "Data", "UserActionTime" };
		TypeInformation[] types = new TypeInformation[] { Types.STRING, Types.STRING, Types.SQL_TIMESTAMP };
		return Types.ROW_NAMED(names, types);
	}

	@Override
	public TableSchema getTableSchema() {
		String[] fields = new String[] { "Username", "Data", "UserActionTime" };
		TypeInformation[] types = new TypeInformation[] { Types.STRING, Types.STRING, Types.SQL_TIMESTAMP };
		return new TableSchema(fields, types);
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
		List<Row> list = new ArrayList<Row>();
		Row row = new Row(3);
		row.setField(0, "A");
		row.setField(1, "data-a");
		row.setField(2, new Timestamp(System.currentTimeMillis()));
		list.add(row);
		Row row1 = new Row(3);
		row1.setField(0, "B");
		row1.setField(1, "data-b");
		row1.setField(2, new Timestamp(System.currentTimeMillis()));
		list.add(row1);
		return env.fromCollection(list);
	}

	/**
	 * 返回带有添加谓词的TableSource的副本。 谓词参数是“提供”给TableSource的可变谓词的可变列表。 
	 * TableSource接受通过从列表中删除谓词来评估谓词。 列表中剩余的谓词将由后续过滤器运算符进行评估。
	 */
	@Override
	public TableSource<Row> applyPredicate(List arg0) {
		return null;
	}

	/**
	 * 如果之前调用了applyPredicate()方法，则返回true。 
	 * 因此，对于从applyPredicate()调用返回的所有TableSource实例，isFilterPushedDown()必须返回true
	 */
	@Override
	public boolean isFilterPushedDown() {
		return true;
	}

	

}
