package com.xzq.flink.table.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * 用户访问记录
 * @author dbnaxlc
 *
 */
@Getter
@Setter
@AllArgsConstructor
@ToString
@NoArgsConstructor
public class Trade {

	private String catg;
	
	private Integer cnt;
	
	private Long ts;
	
	private Double price;
	
}
