package com.xzq.flink.datastream.async;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.ExecutorUtils;

/**
 * 异步IO
 * 
 * @author 夏志强
 * @date 2019年6月4日 上午10:30:40
 */
public class MyAsyncFunction extends RichAsyncFunction<Integer, String> {

	private static final long serialVersionUID = -1420689843159182595L;

	private transient ExecutorService executorService;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		executorService = Executors.newFixedThreadPool(10);
	}

	@Override
	public void close() throws Exception {
		super.close();
		ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, executorService);
	}

	@Override
	public void asyncInvoke(Integer input, ResultFuture<String> resultFuture) throws Exception {
		executorService.submit(() -> {
			long sleep = (long) (ThreadLocalRandom.current().nextFloat() * 2000);
			try {
				Thread.sleep(sleep);
//				if (ThreadLocalRandom.current().nextFloat() < 0.5) {
//					resultFuture.completeExceptionally(new Exception("wahahahaha..."));
//				} else {
					resultFuture.complete(Collections.singletonList("key-" + (input % 10)));
//				}
			} catch (InterruptedException e) {
				resultFuture.complete(new ArrayList<>(0));
			}
		});
	}

}
