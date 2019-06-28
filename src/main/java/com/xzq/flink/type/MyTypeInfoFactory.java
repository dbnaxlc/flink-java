package com.xzq.flink.type;

import java.lang.reflect.Type;
import java.util.Map;

import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

public class MyTypeInfoFactory extends TypeInfoFactory<MyTypeInfo> {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public TypeInformation<MyTypeInfo> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
		return (TypeInformation)Types.STRING;
	}

}
