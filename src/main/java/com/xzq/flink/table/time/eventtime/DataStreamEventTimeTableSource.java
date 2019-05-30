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
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;
import org.apache.flink.types.Row;

import com.xzq.flink.table.dto.Trade;

/**
 * 添加事件时间处理列
 * 
 * @author XIAZHIQIANG
 *
 */
public class DataStreamEventTimeTableSource implements StreamTableSource<Row>, DefinedRowtimeAttributes {

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
	 * 返回RowtimeAttributeDescriptor的列表。 
	 * RowtimeAttributeDescriptor描述具有以下属性的RowTime属性：
	 * 	attributeName: 表模式中的rowtime属性的名称。 必须使用Types.SQL_TIMESTAMP类型定义该字段。
	 * 	timestampExtractor: 时间戳提取器从具有返回类型的记录中提取时间戳。 例如，它可以将Long字段转换为时间戳或解析字符串编码的时间戳。 
	 * 		(Flink附带了一组针对常见用例的内置TimestampExtractor实现。 还可以提供自定义实现。)
	 * 	watermarkStrategy：水位线策略定义了如何为RowTime属性生成水位线。 
	 * 		(Flink附带了一组针对常见用例的内置WatermarkStrategy实现。 还可以提供自定义实现。)
	 */
	@Override
	public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
		RowtimeAttributeDescriptor rowtimeAttrDescr = new RowtimeAttributeDescriptor("UserActionTime",
				new ExistingField("UserActionTime"), new AscendingTimestamps());
		List<RowtimeAttributeDescriptor> listRowtimeAttrDescr = Collections.singletonList(rowtimeAttrDescr);
		return listRowtimeAttrDescr;
	}

}
