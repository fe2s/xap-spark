package com.gigaspaces.spark.streaming.wordcounter.controller;

import com.gigaspaces.spark.streaming.wordcounter.WordCount;
import com.gigaspaces.spark.streaming.wordcounter.service.WordCountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

/**
 * @author Mykola_Zalyayev
 */
@Controller
public class WordCountController {
    @Autowired
    private WordCountService service;

    @RequestMapping(value = "/wordCountChartData", method = RequestMethod.GET)
    @ResponseBody
    public WordCount[] getOverallChartDataReport() {
        return service.getWordCountReport();
    }

}
