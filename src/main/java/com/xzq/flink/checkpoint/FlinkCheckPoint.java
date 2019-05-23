package com.xzq.flink.checkpoint;

import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCheckPoint {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		env.setStateBackend(new MemoryStateBackend());
		//当程序关闭时，会触发额外的Checkpoints
		env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
	}

}
