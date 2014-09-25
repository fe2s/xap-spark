package com.gigaspaces.spark.streaming;

import com.gigaspaces.annotation.pojo.FifoSupport;
import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;

import java.io.Serializable;

/**
 * @author Oleksiy Dyagilev
 */
@SpaceClass(fifoSupport = FifoSupport.OPERATION)
public class Sentence implements Serializable {

    private String id;
    private String text;

    public Sentence() {
    }

    public Sentence(String text) {
        this.text = text;
    }

    @SpaceId(autoGenerate = true)
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return "Sentence{" +
                "id='" + id + '\'' +
                ", text='" + text + '\'' +
                '}';
    }
}
