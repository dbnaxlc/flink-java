package com.xzq.flink.table.tablesource;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import com.xzq.flink.table.dto.Trade;

public class DataStreamTableSourceTest {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);
		List<Trade> list = new ArrayList<Trade>();
		list.add(new Trade("A", 12, 1559035787092l, 3.0));
		list.add(new Trade("B", 2, 1559035987192l, 4.1));
		list.add(new Trade("B", 1, 1559036787027l, 4.5));
		list.add(new Trade("C", 2, 1559037787032l, 4.0));
		list.add(new Trade("A", 2, 1559038787392l, 5.0));
		list.add(new Trade("C", 12, 1559047787092l, 2.5));
		tableEnv.registerDataStream("trade-info", env.fromCollection(list));
//		tableEnv.registerTableSource("trade-info", new DataStreamTableSource());
		
		Table trade = tableEnv.scan("trade-info");
		Table max = tableEnv.scan("trade-info").select("MAX(cnt) as cntm");
		Table min = tableEnv.scan("trade-info").select("MIN(price) as pricem");
		Table result = trade.join(max).where("cnt = cntm").join(min).where("price = pricem").select("catg, cnt, ts, price");
		tableEnv.toRetractStream(result, Trade.class).print();
		env.execute();

	}

}
