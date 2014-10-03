package com.gigaspaces.spark.streaming.wordcounter.service;

import com.gigaspaces.spark.streaming.wordcounter.WordCount;
import org.openspaces.core.GigaSpace;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

/**
 * @author Mykola_Zalyayev
 */
@Service
public class WordCountService {

    @Autowired
    private GigaSpace space;


    public List<WordCount> getWordCountReport() {
        return Arrays.asList(space.readMultiple(new WordCount()));
    }
}
