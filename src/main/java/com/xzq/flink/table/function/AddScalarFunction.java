package com.xzq.flink.table.function;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * 标量函数，对单个或者多个输入字段计算后返回一个确定类型的标量值。
 * 类型不能是：TEXT，NTEXT，IMAGE，TIMESTAMP，CURSOR，TABLE
 * @author dbnaxlc
 * @date 2019年6月12日 上午11:04:59
 */
public class AddScalarFunction extends ScalarFunction {

	private static final long serialVersionUID = -1752517028177967936L;
	
	public Integer eval(Integer a, Integer b) {
		return a + b;
	}

	@Override
	public TypeInformation<?>[] getParameterTypes(Class<?>[] signature) {
		return new TypeInformation[] {Types.INT, Types.INT};
	}

	@Override
	public TypeInformation<?> getResultType(Class<?>[] signature) {
		return Types.INT;
	}

	
}
