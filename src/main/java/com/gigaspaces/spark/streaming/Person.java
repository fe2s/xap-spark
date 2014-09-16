package com.gigaspaces.spark.streaming;

import com.gigaspaces.annotation.pojo.FifoSupport;
import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;

import java.io.Serializable;

/**
 * @author Oleksiy Dyagilev
 */
@SpaceClass(fifoSupport = FifoSupport.OPERATION)
public class Person implements Serializable {

    private String id;
    private String name;

    @SpaceId(autoGenerate = true)
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void test() {
        System.out.println("test");
    }
}
