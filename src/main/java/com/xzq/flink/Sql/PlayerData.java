package com.xzq.flink.Sql;

public class PlayerData {

	/**
	 * 赛季，球员，出场，首发，时间，助攻，抢断，盖帽，得分
	 */
	public String season;
	public String player;
	public String play_num;
	public Integer first_court;
	public Double time;
	public Double assists;
	public Double steals;
	public Double blocks;
	public Double scores;

	public PlayerData() {
		super();
	}

	public PlayerData(String season, String player, String play_num, Integer first_court, Double time, Double assists,
			Double steals, Double blocks, Double scores) {
		this.season = season;
		this.player = player;
		this.play_num = play_num;
		this.first_court = first_court;
		this.time = time;
		this.assists = assists;
		this.steals = steals;
		this.blocks = blocks;
		this.scores = scores;
	}
}
