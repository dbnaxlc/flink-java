package com.xzq.flink.cep;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FirstCep {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Event> input = env.fromElements(new Event(1, "zhangsan"), new Event(2, "lisi"), new Event(3, "wangwu"),
				new SubEvent(4, "paozi", 53d), new Event(5, "wah"), new Event(6, "wah11"));
		Pattern<Event, Event> pattern = Pattern.<Event>begin("start").times(1,3).where(new SimpleCondition<Event>() {
			
			private static final long serialVersionUID = 7372966384104104892L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getId() > 2;
			}
		}).next("middle").subtype(SubEvent.class).where(new SimpleCondition<SubEvent>() {
			
			private static final long serialVersionUID = -7285955150678385969L;

			@Override
			public boolean filter(SubEvent value) throws Exception {
				return value.getVolume() > 10;
			}
		}).followedBy("end").where(new SimpleCondition<Event>() {
			
			private static final long serialVersionUID = -7771954202128275810L;

			@Override
			public boolean filter(Event event) {
				return event.getName().equals("wah");
			}
		});
		PatternStream<Event> ps = CEP.pattern(input, pattern);
		ps.select(new PatternSelectFunction<Event, List<Tuple3<Integer, String, Double>>>() {

			private static final long serialVersionUID = 3646532078294677050L;

			@Override
			public List<Tuple3<Integer, String, Double>> select(Map<String, List<Event>> pattern) throws Exception {
				List<Tuple3<Integer, String, Double>> list = new ArrayList<>();
				Collection<List<Event>> values = pattern.values();
				for(List<Event> events : values) {
					for(Event event: events) {
						if(event instanceof SubEvent) {
							SubEvent se = (SubEvent) event;
							list.add(new Tuple3<Integer, String, Double>(se.getId(), se.getName(), se.getVolume()));
						} else {
							list.add(new Tuple3<Integer, String, Double>(event.getId(), event.getName(), 0d));
						}
					}
				}
				return list;
			}
		}).print();
		env.execute("first-cep");
	}

}
