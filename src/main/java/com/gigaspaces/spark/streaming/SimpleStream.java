package com.gigaspaces.spark.streaming;

/**
 * @author Oleksiy Dyagilev
 */

import com.gigaspaces.client.TakeModifiers;
import org.openspaces.core.GigaSpace;

import java.io.Serializable;
import java.util.List;

/**
 * Simple stream with ability to write and read data.
 * Works with arbitrary space class that has FifoSupport.OPERATION annotation and implements Serializable.
 *
 * @author Oleksiy_Dyagilev
 */
public class SimpleStream<T extends Serializable> {

    private GigaSpace space;
    private T template;

    /**
     * Creates a reference to stream
     *
     * @param space space
     * @param template used to match objects during reading. If you want to have several streams with the same type,
     *                 template objects should differentiate your streams.
     */
    public SimpleStream(GigaSpace space, T template) {
        this.space = space;
        this.template = template;
    }

    /**
     * write single element to stream
     * @param value element
     */
    public void write(T value){
        space.write(value);
    }

    /**
     * write batch of elements to stream
     * @param values list of elements
     */
    public void writeBatch(List<T> values) {
        space.writeMultiple(values.toArray());
    }

    /**
     * read and remove single element from stream
     * @return read element
     */
    public T read() {
        return space.take(template, 0L, TakeModifiers.FIFO);
    }

    /**
     * read and remove batch of elements from stream
     *
     * @param maxNumber max number of elements
     * @return read elements
     */
    public T[] readBatch(int maxNumber){
        return space.takeMultiple(template, maxNumber, TakeModifiers.FIFO);
    }

}
