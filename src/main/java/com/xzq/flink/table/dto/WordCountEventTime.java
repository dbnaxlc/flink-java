package com.xzq.flink.table.dto;

import java.sql.Timestamp;

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
public class WordCountEventTime {
    public String word;
    public long frequency;
    
    public long UserActionTime;

    

    
}
