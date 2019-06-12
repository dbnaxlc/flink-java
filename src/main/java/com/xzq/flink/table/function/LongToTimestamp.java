package com.xzq.flink.table.function;

import java.sql.Timestamp;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;

public class LongToTimestamp extends ScalarFunction {

	private static final long serialVersionUID = 520155912224141139L;

	public Timestamp eval(Long a) {
		return new Timestamp(a);
	}

	@Override
	public TypeInformation<?>[] getParameterTypes(Class<?>[] signature) {
		return new TypeInformation[] {Types.LONG};
	}

	@Override
	public TypeInformation<?> getResultType(Class<?>[] signature) {
		return Types.SQL_TIMESTAMP;
	}
}
