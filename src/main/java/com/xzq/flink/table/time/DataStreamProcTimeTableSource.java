package com.xzq.flink.table.time;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.StreamTableSource;

import com.xzq.flink.table.dto.Trade;

/**
 * 添加处理时间处理列
 * @author XIAZHIQIANG
 *
 */
public class DataStreamProcTimeTableSource implements StreamTableSource<Trade>, DefinedProctimeAttribute{

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
	public TypeInformation<Trade> getReturnType() {
		return Types.POJO(Trade.class);
	}

	@Override
	public TableSchema getTableSchema() {
//		添加自定义处理时间列
		String[] fields = { "catg", "cnt", "ts", "price", "DateProcTime" };
		TypeInformation[] fieldTypes = { Types.STRING, Types.INT, Types.LONG, Types.DOUBLE, Types.SQL_TIMESTAMP };
		return new TableSchema(fields, fieldTypes);
	}

	@Override
	public DataStream<Trade> getDataStream(StreamExecutionEnvironment env) {
		List<Trade> list = new ArrayList<Trade>();
		list.add(new Trade("A", 12, 1559035787092l, 3.0));
		list.add(new Trade("B", 2, 1559035987192l, 4.1));
		list.add(new Trade("B", 1, 1559036787027l, 4.5));
		list.add(new Trade("C", 2, 1559037787032l, 4.0));
		list.add(new Trade("A", 2, 1559038787392l, 5.0));
		list.add(new Trade("C", 12, 1559047787092l, 2.5));
		return env.fromCollection(list);
	}

	@Override
	public String getProctimeAttribute() {
		// 追加为另外一个列
        return "DateProcTime";
	}

}
