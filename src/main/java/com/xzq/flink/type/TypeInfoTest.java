package com.xzq.flink.type;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

public class TypeInfoTest {

	public static void main(String[] args) {
		TypeInformation<MyTypeInfo> wxc = TypeExtractor.createTypeInfo(MyTypeInfo.class);
		System.out.println(wxc);
	}

}
