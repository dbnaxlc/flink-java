package com.xzq.flink.json.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@AllArgsConstructor
@ToString
@NoArgsConstructor
public class Result {

	private Integer id;
	
	private Item[] items;
}
