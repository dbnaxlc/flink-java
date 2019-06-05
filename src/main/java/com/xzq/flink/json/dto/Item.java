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
public class Item {
	private Integer id;
	private String name;
	private String title;
	private String url;
	private long publish_time;
	private float score;
}
