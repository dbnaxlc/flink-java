package com.xzq.flink.datastream;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import com.xzq.flink.dto.WordCount;

/**
 * 通过key选择器指定key
 * @author XIAZHIQIANG
 *
 */
public class KeySelectorTest {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//开启avro序列化
//		env.getConfig().enableForceAvro();
		env.fromElements("wahah-1","wahah-2","wahah-3","wahah-4","wahah-5","wahah-6","wahah-7","wahah-8","wahah-9")
				.flatMap((String value, Collector<WordCount> out) -> {
					String[] mkv = value.split("-");
					out.collect(new WordCount(mkv[0], Long.parseLong(mkv[1])));
				})
				// 推断不出来返回的集合是SimpleTuple的泛型。只能通过returns()明确返回类型
				.returns(WordCount.class).keyBy(new KeySelector<WordCount, String>(){

					private static final long serialVersionUID = -4862196772086713932L;

					@Override
					public String getKey(WordCount value) throws Exception {
						return value.word;
					}})
				// 每2s，计算最近3s窗口数据
				.timeWindow(Time.seconds(3), Time.seconds(2))
				.sum("frequency").print();
		env.execute("tubling-time-window");

	}

}
