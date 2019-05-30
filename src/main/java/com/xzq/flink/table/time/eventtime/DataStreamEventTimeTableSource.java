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
	 * 		1、TimestampExtractor目前提供以下实现：
	 * 			1.1、ExistingField(fieldName):从现有的LONG，SQL_TIMESTAMP或时间戳格式的STRING字段中提取rowtime属性的值。 这种字符串的一个例子是'2018-05-28 12：34：56.000'。
	 * 			1.2、StreamRecordTimestamp()：从DataStream StreamRecord的时间戳中提取rowtime属性的值。 请注意，此TimestampExtractor不适用于批处理表源。
	 * 			1.3、自定义时间戳提取器(TimestampExtractor)可以通过实现相应的接口来定义
	 * 	watermarkStrategy：水位线策略定义了如何为RowTime属性生成水位线。 
	 * 		2、WatermarkStrategy目前提供以下实现：
	 * 			2.1、AscendingTimestamps：用于提升时间戳的水位线策略。带有时间戳的记录如果顺序错误，将被认为是延迟的。
	 * 			2.2、BoundedOutOfOrderTimestamps(delay)：时间戳的水位线策略，其最多在指定的延迟之外是无序的。
	 * 			2.3、PreserveWatermarks()：一种表示水位线应该从基础数据流中保留的策略。
	 * 			2.4、自定义水位线策略(WatermarkStrategy)可以通过实现相应的接口来定义。
	 * 目前只支持单个Rowtime属性
	 * StreamTableSource和BatchTableSource都可以实现DefinedRowtimeAttributes并定义rowtime属性。 
	 * 在任何一种情况下，都使用TimestampExtractor提取rowtime字段。 
	 * 因此，实现StreamTableSource和BatchTableSource并定义rowtime属性的TableSource为流式和批量查询提供完全相同的数据
	 */
	@Override
	public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
		RowtimeAttributeDescriptor rowtimeAttrDescr = new RowtimeAttributeDescriptor("UserActionTime",
				new ExistingField("UserActionTime"), new AscendingTimestamps());
		List<RowtimeAttributeDescriptor> listRowtimeAttrDescr = Collections.singletonList(rowtimeAttrDescr);
		return listRowtimeAttrDescr;
	}

}
