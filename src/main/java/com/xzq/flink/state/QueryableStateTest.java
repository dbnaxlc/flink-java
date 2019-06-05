package com.xzq.flink.state;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.queryablestate.client.QueryableStateClient;

public class QueryableStateTest {

	public static void main(String[] args) throws IOException {
		JobID jobId = JobID.fromHexString("156456465464");
		QueryableStateClient client = new QueryableStateClient("localhost", 9067);
		ValueStateDescriptor<Double> descriptor =
                new ValueStateDescriptor<Double>(
                        "average",
                        TypeInformation.of(new TypeHint<Double>() {}));

        CompletableFuture<ValueState<Double>> resultFuture =
        		client.getKvState(jobId, "my-query", 1d, BasicTypeInfo.DOUBLE_TYPE_INFO, descriptor);

        ValueState<Double> res = resultFuture.join();
        System.out.println(res.value());
        client.shutdownAndWait();
	}

}
