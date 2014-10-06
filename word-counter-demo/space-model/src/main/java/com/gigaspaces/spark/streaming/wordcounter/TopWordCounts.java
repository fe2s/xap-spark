package com.gigaspaces.spark.streaming.wordcounter;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;

import java.util.Arrays;
import java.util.List;

/**
 * @author Oleksiy Dyagilev
 */
@SpaceClass
public class TopWordCounts {
    private Integer id = 1;

    private WordCount[] wordCounts;

    public TopWordCounts() {
    }

    public TopWordCounts(WordCount[] wordCounts) {
        this.wordCounts = wordCounts;
    }

    @SpaceId
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public WordCount[] getWordCounts() {
        return wordCounts;
    }

    public void setWordCounts(WordCount[] wordCounts) {
        this.wordCounts = wordCounts;
    }

    @Override
    public String toString() {
        return "TopWordCounts{" +
                "id=" + id +
                ", wordCounts=" + Arrays.toString(wordCounts) +
                '}';
    }
}
