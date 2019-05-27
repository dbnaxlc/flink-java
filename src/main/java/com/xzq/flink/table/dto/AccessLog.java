package com.xzq.flink.table.dto;

import java.sql.Date;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * 用户访问记录
 * @author XIAZHIQIANG
 *
 */
@Getter
@Setter
@AllArgsConstructor
@ToString
@NoArgsConstructor
public class AccessLog {

	private String region;
	
	private String userId;
	
	private Date accessTime;
}
