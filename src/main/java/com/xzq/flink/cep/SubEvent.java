package com.xzq.flink.cep;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@ToString
public class SubEvent extends Event {

	private Double volume;
	
	public SubEvent(Integer id, String name, Double volume) {
		this.volume = volume;
		this.setId(id);
		this.setName(name);
	}
}
