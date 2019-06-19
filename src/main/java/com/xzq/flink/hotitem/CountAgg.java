package com.xzq.flink.hotitem;

import org.apache.flink.api.common.functions.AggregateFunction;

public class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

	private static final long serialVersionUID = 1536211899331841035L;

	@Override
	  public Long createAccumulator() {
	    return 0L;
	  }

	  @Override
	  public Long add(UserBehavior userBehavior, Long acc) {
	    return acc + 1;
	  }

	  @Override
	  public Long getResult(Long acc) {
	    return acc;
	  }

	  @Override
	  public Long merge(Long acc1, Long acc2) {
	    return acc1 + acc2;
	  }
	}