package com.xzq.flink.table.dto;

public class WordCount {
    public String word;
    public long frequency;

    public WordCount() {}

    public WordCount(String word, long frequency) {
        this.word = word;
        this.frequency = frequency;
    }

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public long getFrequency() {
		return frequency;
	}

	public void setFrequency(long frequency) {
		this.frequency = frequency;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("WordCount [word=");
		builder.append(word);
		builder.append(", frequency=");
		builder.append(frequency);
		builder.append("]");
		return builder.toString();
	}

    
}
