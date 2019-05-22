package com.xzq.flink.window.time;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import com.xzq.flink.tuple.SimpleTuple;

/**
 * 滚动时间窗口
 * @author dbnaxlc
 * @date 2019年5月22日 上午11:41:59
 */
public class TumblingCountWindow {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.fromElements("wahah-1,wahah-2,wahah-3,wahah-4,wahah-5,wahah-6,wahah-7,wahah-8,wahah-9")
				.flatMap((String value, Collector<SimpleTuple> out) -> {
					String[] strs = value.split(",");
					for (String str : strs) {
						String[] mkv = str.split("-");
						out.collect(new SimpleTuple(mkv[0], Integer.parseInt(mkv[1])));
						if(Integer.parseInt(mkv[1]) % 3 == 0) {
							Thread.sleep(2000l);
						}
					}
				})
				// 推断不出来返回的集合是SimpleTuple的泛型。只能通过returns()明确返回类型
				.returns(Types.TUPLE(SimpleTuple.class)).keyBy(0)
				// 每2秒没计算窗口内总数
				.timeWindow(Time.seconds(2))
				.sum(1).print();
		env.execute("tubling-time-window");
	}

}
