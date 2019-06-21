package com.xzq.flink.table.function;

import java.util.Iterator;

import org.apache.flink.table.functions.AggregateFunction;

import com.xzq.flink.table.dto.SumAvg;

public class SumAvgAggregateFunction extends AggregateFunction<Integer, SumAvg> {

	private static final long serialVersionUID = 8090600094789954259L;

	@Override
	public SumAvg createAccumulator() {
		return new SumAvg();
	}

	@Override
	public Integer getValue(SumAvg sa) {
		if(sa.getNumber1() == 0) {
			return 0;
		} 
		return sa.getNumber2() / sa.getNumber1();
	}
	
	public void accumulate(SumAvg acc, int iValue, int iWeight) {
        acc.setNumber2(acc.getNumber2() + iValue * iWeight);
        acc.setNumber1(acc.getNumber1() + iWeight);
    }

	/**
	 * 在bounded OVER窗口的聚合方法中是需要实现的
     * @author dbnaxlc
	 * @date 2019年6月20日 上午9:58:10
	 * @param acc
	 * @param iValue
	 * @param iWeight
	 */
    public void retract(SumAvg acc, int iValue, int iWeight) {
    	acc.setNumber2(acc.getNumber2() - iValue * iWeight);
        acc.setNumber1(acc.getNumber1() - iWeight);
    }
    
    /**
     * 在很多batch 聚合和会话窗口聚合是必须的
     * @author dbnaxlc
     * @date 2019年6月20日 上午9:58:32
     * @param acc
     * @param it
     */
    public void merge(SumAvg acc, Iterable<SumAvg> it) {
        Iterator<SumAvg> iter = it.iterator();
        while (iter.hasNext()) {
            SumAvg a = iter.next();
            acc.setNumber2(acc.getNumber2() + a.getNumber2());
            acc.setNumber1(acc.getNumber1() + a.getNumber1());
        }
    }
    
    /**
     * 在大多数batch聚合是必须的
     * @author dbnaxlc
     * @date 2019年6月20日 上午9:58:36
     * @param acc
     */
    public void resetAccumulator(SumAvg acc) {
        acc.setNumber1(0);
        acc.setNumber2(0);
    }

}
