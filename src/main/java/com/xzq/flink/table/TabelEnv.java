package com.xzq.flink.table;



import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.alibaba.fastjson.JSONObject;

public class TabelEnv {

		private static final String KAFKATOPIC = "kafak_source_topic";
		 
		 
	    private static final String KAFKASERVER = "dev.kafka.com";
	 
	 
	    public static void main(String[] args) throws Exception {
	        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	        org.apache.flink.table.api.java.StreamTableEnvironment sTableEnv = TableEnvironment.getTableEnvironment(env);
	        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
	 
	        Properties kafkaProperties = new Properties();
	        kafkaProperties.setProperty("bootstrap.servers", KAFKASERVER);
	        kafkaProperties.setProperty("group.id", "TableApiJob");
	 
	        String schema = "{\"id\":\"int\",\"name\":\"string\",\"score\":\"int\",\"currentTimeStamp\":\"long\"}";
	        JSONObject jsonObject = JSONObject.parseObject(schema);
	 
	        //字典映射
	        Map<String, TypeInformation> dic = new HashMap<>();
	        dic.put("string", Types.STRING());
	        dic.put("int", Types.INT());
	        dic.put("long", Types.LONG());
	 
	        Set<String> keySet = jsonObject.keySet();
	 
	 
	        String[] key = (String[]) keySet.toArray(new String[keySet.size()]);
	 
	 
	        List<TypeInformation> valueList = new ArrayList<>();
	        for (String i : keySet) {
	            valueList.add(dic.get(jsonObject.getString(i)));
	        }
	 
	        TypeInformation<?>[] value = (TypeInformation<?>[]) valueList.toArray(new TypeInformation<?>[valueList.size()]);
	 
	        // specify JSON field names and types
	        TypeInformation<Row> typeInfo = Types.ROW(
	            key,
	            value
	        );
	        
	        Properties pro = new Properties();
			pro.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.5.120:9092,172.16.5.140:9092,172.16.5.223:9092");
			pro.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "xzq0521");
			env.addSource(new FlinkKafkaConsumer<String>("xzq0426", new SimpleStringSchema(), pro));
	 
	 
	        env.execute();
	 
	    }


}
