package com.xzq.flink.hotitem;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class HotItems {

	/**
	 * @author 夏志强
	 * @date 2019年6月19日 下午3:31:22
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		Path filePath = Path
				.fromLocalFile(new File("G:\\git\\my-flink-project\\src\\main\\resources\\UserBehavior.csv"));
		// 抽取 UserBehavior 的 TypeInformation，是一个 PojoTypeInfo
		PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo<UserBehavior>) TypeExtractor
				.createTypeInfo(UserBehavior.class);
		// 由于 Java 反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
		String[] fieldOrder = new String[] { "userId", "itemId", "categoryId", "behavior", "timestamp" };
		// 创建 PojoCsvInputFormat
		PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);
		DataStream<UserBehavior> dataStream = env.createInput(csvInput, pojoType);
		DataStream<UserBehavior> timeStream = dataStream.assignTimestampsAndWatermarks(
				new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.seconds(0)) {

					private static final long serialVersionUID = -2134813610700483846L;

					@Override
					public long extractTimestamp(UserBehavior element) {
						return element.timestamp * 1000l;
					}
				});
		DataStream<UserBehavior> pvData = timeStream.filter(item -> item.behavior.equals("pv"));
		DataStream<ItemViewCount> windowedData = pvData.keyBy("itemId").timeWindow(Time.minutes(60), Time.minutes(5))
				.aggregate(new CountAgg(), new WindowResultFunction());
		windowedData.keyBy("windowEnd").process(new KeyedProcessFunction<Tuple, ItemViewCount, String>() {

			private static final long serialVersionUID = 1176544666504400938L;

			private final int topSize = 3;

			// 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
			private ListState<ItemViewCount> itemState;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				ListStateDescriptor<ItemViewCount> des = new ListStateDescriptor<>("item-view", ItemViewCount.class);
				itemState = getRuntimeContext().getListState(des);
			}

			@Override
			public void processElement(ItemViewCount item,
					KeyedProcessFunction<Tuple, ItemViewCount, String>.Context context, Collector<String> coll)
					throws Exception {
				itemState.add(item);
				// 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
				context.timerService().registerEventTimeTimer(item.windowEnd + 1);
			}

			@Override
			public void onTimer(long timestamp, KeyedProcessFunction<Tuple, ItemViewCount, String>.OnTimerContext ctx,
					Collector<String> out) throws Exception {
				// 获取收到的所有商品点击量
				List<ItemViewCount> allItems = new ArrayList<>();
				for (ItemViewCount item : itemState.get()) {
					allItems.add(item);
				}
				// 提前清除状态中的数据，释放空间
				itemState.clear();
				// 按照点击量从大到小排序
				allItems.sort(new Comparator<ItemViewCount>() {
					@Override
					public int compare(ItemViewCount o1, ItemViewCount o2) {
						return (int) (o2.viewCount - o1.viewCount);
					}
				});
				// 将排名信息格式化成 String, 便于打印
				StringBuilder result = new StringBuilder();
				result.append("====================================\n");
				result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n");
				for (int i = 0; i < topSize; i++) {
					ItemViewCount currentItem = allItems.get(i);
					// No1: 商品ID=12224 浏览量=2413
					result.append("No").append(i).append(":").append("  商品ID=").append(currentItem.itemId)
							.append("  浏览量=").append(currentItem.viewCount).append("\n");
				}
				result.append("====================================\n\n");

				out.collect(result.toString());
			}

		}).print();

		env.execute("hot-item");
	}

}
