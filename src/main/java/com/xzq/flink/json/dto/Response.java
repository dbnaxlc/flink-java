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
public class Response {

	private Long search_time;
    private Integer code;
    private Result results;
}
