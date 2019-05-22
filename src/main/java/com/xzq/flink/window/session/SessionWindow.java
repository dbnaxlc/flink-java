package com.xzq.flink.window.session;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import com.xzq.flink.tuple.SimpleTuple;

/**
 * 会话窗口，采用会话持续时长作为窗口处理依据。设置指定的会话持续时长时间，在这段时间中不再出现会话则认为超出会话时长。
 * @author dbnaxlc
 * @date 2019年5月22日 上午10:21:10
 */
public class SessionWindow {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.fromElements("wahah-1,wahah-2,wahah-3,wahah-4,wahah-5,wahah-6,wahah-7,wahah-8,wahah-9")
				.flatMap((String value, Collector<SimpleTuple> out) -> {
					String[] strs = value.split(",");
					for (String str : strs) {
						String[] mkv = str.split("-");
						out.collect(new SimpleTuple(mkv[0], Integer.parseInt(mkv[1])));
						if(Integer.parseInt(mkv[1]) % 3 == 0) {
							Thread.sleep(2001l);
						}
					}
				})
				// 推断不出来返回的集合是SimpleTuple的泛型。只能通过returns()明确返回类型
				.returns(Types.TUPLE(SimpleTuple.class)).keyBy(0)
				// 每超过2秒没有事件时计算窗口内总数
				.window(ProcessingTimeSessionWindows.withGap(Time.seconds(2)))
				.sum(1).print();
		env.execute("session-window");

	}

}
