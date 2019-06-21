package com.xzq.flink.table.function;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * 标量函数，是指返回一个值的函数。对0/单个或者多个输入字段计算后返回一个确定类型的标量值。
 * 类型不能是：TEXT，NTEXT，IMAGE，TIMESTAMP，CURSOR，TABLE
 * @author dbnaxlc
 * @date 2019年6月12日 上午11:04:59
 */
public class AddScalarFunction extends ScalarFunction {

	private static final long serialVersionUID = -1752517028177967936L;
	
	/**
	 * 在evaluation方法调用前调用一次
	 * 通过FunctionContext可以获取udf执行环境的上下文，比如，metric group，分布式缓存文件，全局的job参数。
	 */
	@Override
	public void open(FunctionContext context) throws Exception {
		super.open(context);
		System.out.println(context.getJobParameter("db.url", "localhost"));
	}
	
	/**
	 * 在evaluation方法最后一次调用后调用。
	 */
	@Override
	public void close() throws Exception {
		super.close();
		System.out.println("close");
	}



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
