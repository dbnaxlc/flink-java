package com.xzq.flink.table.time.proctime;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.WindowedTable;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;

import com.xzq.flink.table.dto.TradeProcTime;

/**
 * 在从DataStream或者TableSource创建Table时可以指定Time Attributes，指定了之后就可以作为field来使用或者参与time-based的操作
 * 1、Processing time：
 * 	   1.1、从DataStream创建Table的话，可以在fromDataStream里头进行定义；
 *     1.2、从TableSource创建Table的话，可以通过实现DefinedProctimeAttribute接口来定义Processing time；
 *          DefinedProctimeAttribute定义了getProctimeAttribute方法，返回String，用于定义Process time的字段名
 * 2、Event time：
 *     2.1、从DataStream创建Table的话，可以在fromDataStream里头进行定义；具体有两种方式，一种是额外定义一个字段，一种是覆盖原有的字段；
 *     2.2、从TableSource创建Table的话，可以通过实现DefinedRowtimeAttributes接口来定义Event time；
 *          DefinedRowtimeAttributes定义了getRowtimeAttributeDescriptors方法，返回的是RowtimeAttributeDescriptor的List，RowtimeAttributeDescriptor有3个属性，分别是attributeName、timestampExtractor及watermarkStrategy
 * @author XIAZHIQIANG
 *
 */
public class DataStreamTableSourceProcTimeAttribute {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);
		tableEnv.registerTableSource("table-time-attr", new DataStreamProcTimeTableSource());
		WindowedTable table = tableEnv.scan("table-time-attr").window(Tumble.over("10.minutes").on("DateProcTime").as("userActionWindow"));
		tableEnv.toRetractStream(table.table(), TradeProcTime.class).print();
		env.execute();
	}

}
