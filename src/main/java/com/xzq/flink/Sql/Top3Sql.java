package com.xzq.flink.Sql;

import java.io.File;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public class Top3Sql {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);
//		DataSet<PlayerData> pdSet = getDataSetByReadTextFile(env);
//		DataSet<PlayerData> pdSet = getDataSetByCreateInput(env);
		DataSet<PlayerData> pdSet = getDataSetByReadCsvFile(env);
		tableEnv.registerDataSet("player_data", pdSet);
		Table table = tableEnv.sqlQuery("select player, count(season) as num from player_data group by player order by num desc limit 3");
		DataSet<Result> result = tableEnv.toDataSet(table, Result.class);
		result.print();
	}

	public static DataSet<PlayerData> getDataSetByReadTextFile(ExecutionEnvironment env) {
		DataSet<String> dataSet = env.readTextFile(Top3Sql.class.getClassLoader().getResource("score.txt").getPath());
		DataSet<PlayerData> pdSet = dataSet.map(str -> {
			String[] datas = str.split(",");
			return new PlayerData(String.valueOf(datas[0]),
                    String.valueOf(datas[1]),
                    String.valueOf(datas[2]),
                    Integer.valueOf(datas[3]),
                    Double.valueOf(datas[4]),
                    Double.valueOf(datas[5]),
                    Double.valueOf(datas[6]),
                    Double.valueOf(datas[7]),
                    Double.valueOf(datas[8])
            );
		});
		return pdSet;
	}
	
	public static DataSet<PlayerData> getDataSetByReadCsvFile(ExecutionEnvironment env) {
		DataSet<PlayerData> pdSet = env.readCsvFile(Top3Sql.class.getClassLoader().getResource("score.txt").getPath())
				.fieldDelimiter(",").pojoType(PlayerData.class, "season","player","play_num","first_court","time","assists","steals","blocks","scores");
		return pdSet;
	}
	
	public static DataSet<PlayerData> getDataSetByCreateInput(ExecutionEnvironment env) {
		PojoTypeInfo<PlayerData> typeInfo = (PojoTypeInfo<PlayerData>) TypeExtractor.createTypeInfo(PlayerData.class);
		String[] fieldNames = {"season","player","play_num","first_court","time","assists","steals","blocks","scores"};
		Path filePath = Path.fromLocalFile(new File(Top3Sql.class.getClassLoader().getResource("score.txt").getPath()));
		PojoCsvInputFormat<PlayerData> input = new PojoCsvInputFormat<>(filePath, typeInfo, fieldNames);
		DataSet<PlayerData> pdSet = env.createInput(input, typeInfo);
		return pdSet;
	}
}
