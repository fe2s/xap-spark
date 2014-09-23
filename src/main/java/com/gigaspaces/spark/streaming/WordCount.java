package com.gigaspaces.spark.streaming;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;

/**
 * @author Oleksiy Dyagilev
 */
@SpaceClass
public class WordCount {
    private String word;
    private Integer count;

    public WordCount() {
    }

    public WordCount(String word, Integer count) {
        this.word = word;
        this.count = count;
    }

    @SpaceId
    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
}
