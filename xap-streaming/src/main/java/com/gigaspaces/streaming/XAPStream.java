package com.gigaspaces.streaming;

import com.gigaspaces.client.TakeModifiers;
import com.gigaspaces.client.WriteModifiers;
import org.openspaces.core.GigaSpace;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Stream with ability to write and read data.
 * Works with arbitrary space class that has FifoSupport.OPERATION annotation and implements Serializable.
 *
 * @author Oleksiy_Dyagilev
 */
public class XAPStream<T extends Serializable> {

    private GigaSpace space;
    private T template;

    /**
     * Creates a reference to stream
     *
     * @param space    space
     * @param template used to match objects during reading. If you want to have several streams with the same type,
     *                 template objects should differentiate your streams.
     */
    public XAPStream(GigaSpace space, T template) {
        this.space = space;
        this.template = template;
    }

    /**
     * write single element to stream
     *
     * @param value element
     */
    public void write(T value) {
        space.write(value, WriteModifiers.ONE_WAY);
    }

    /**
     * write batch of elements to stream
     *
     * @param values list of elements
     */
    public void writeBatch(List<T> values) {
        space.writeMultiple(values.toArray(), WriteModifiers.ONE_WAY);
    }

    /**
     * read and remove single element from stream
     *
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
    public T[] readBatch(int maxNumber) {
        return space.takeMultiple(template, maxNumber, TakeModifiers.FIFO);
    }

    /**
     * read and remove batch of elements from the stream,
     * if there is nothing to be read, indefinitely waits for data be available by sleeping
     * configured interval of time and checking again
     *
     * TODO: ideally we should use a blocking take() with timeout to wait for data be available,
     * but this API is not supported against clustered Space. Consider creating multiple Space proxies,
     * one per partition and call blocking take() against each of them.
     *
     * @param maxNumber max number of elements
     * @param readRetryInterval interval to wait if there is no objects in the stream before the next read attempt
     * @return read elements
     */
    public T[] readBatch(int maxNumber, long readRetryInterval) {
        T[] items = space.takeMultiple(template, maxNumber, TakeModifiers.FIFO);
        while (items == null || items.length == 0) {
            try {
                Thread.sleep(readRetryInterval);
                items = space.takeMultiple(template, maxNumber, TakeModifiers.FIFO);
            } catch (InterruptedException e) {
            }
        }

        return items;
    }


}
