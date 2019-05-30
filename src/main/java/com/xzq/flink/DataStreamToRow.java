package com.xzq.flink;

import java.util.Iterator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class DataStreamToRow {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
	    environment.getConfig().disableSysoutLogging();
	    StreamTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(environment);
	    DataStream<String> dataStream = environment.addSource(new SourceFunction<String>() {
	        private String str1 = "{\"name\":\"name-value\",\"age\":\"28\",\"sex\":\"1\"}";
	        private long count = 0L;
	        private volatile boolean isRunning = true;
	        @Override
	        public void run(SourceContext<String> ctx) throws Exception {
	            while (isRunning && count<2){
	                synchronized (ctx.getCheckpointLock()){
	                    ctx.collect(str1);
	                    count++;
	                }
	            }
	        }
	        @Override
	        public void cancel() {
	            isRunning = false;
	        }
	    });
	    DataStream<JsonNode> dataStreamJson = dataStream.map(new MapFunction<String, JsonNode>() {
	        @Override
	        public JsonNode map(String s) throws Exception {
	            ObjectMapper objectMapper = new ObjectMapper();
	            JsonNode node = objectMapper.readTree(s);
	            return node;
	        }
	    });
	    DataStream<Row> dataStreamRow = dataStreamJson.map(new MapFunction<JsonNode, Row>() {
	        @Override
	        public Row map(JsonNode jsonNode) throws Exception {
	            int pos = 0;
	            Row row = new Row(jsonNode.size());
	            Iterator<String> iterator = jsonNode.fieldNames();
	            while (iterator.hasNext()){
	                String key = iterator.next();
	                row.setField(pos,jsonNode.get(key).asText());
	                pos++;
	            }
	            return row;
	        }
	    }).returns(new RowTypeInfo(Types.STRING, Types.STRING, Types.STRING));
	    dataStreamRow.addSink(new SinkFunction<Row>() {
	        @Override
	        public void invoke(Row value) throws Exception {
	            System.out.println(value.getField(0));
	        }
	    });
	    Table myTable = tableEnvironment.fromDataStream(dataStreamRow);
	    Table result = myTable.select("f0, f1");
	    TypeInformation<Tuple2<String, String>> dataStreamType = 
	    		TypeInformation.of(new TypeHint<Tuple2<String, String>>() {});
	    DataStream<Tuple2<String, String>> dataStreamResult = tableEnvironment.toAppendStream(
	    		result, dataStreamType);
	    dataStreamResult.print();
	    environment.execute();
	}

}
