package com.xzq.flink.hotitem;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

/** 用户行为数据结构 **/
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class UserBehavior {
  public long userId;         // 用户ID
  public long itemId;         // 商品ID
  public int categoryId;      // 商品类目ID
  public String behavior;     // 用户行为, 包括("pv", "buy", "cart", "fav")
  public long timestamp;      // 行为发生的时间戳，单位秒
}
