package com.xzq.flink.table.dto;

import java.sql.Timestamp;

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
public class TradeProcTime {

	private String catg;
	
	private Integer cnt;
	
	private Long ts;
	
	private Double price;
	
	private Timestamp DateProcTime;
	
}
