/*
 * Copyright 2014 Sean Kerr
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.simplebase.writer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * {@link BufferedContextWriter} writes <em>Put</em> operations to a mapreduce context.
 *
 * <p>
 * Data is written when the put buffer size has been reached, when the table has been changed, or when
 * {@link BufferedContextWriter#flush} is called--whichever comes first.
 * </p>
 *
 * <p>
 * <strong>Note:</strong> {@link Writer#close} calls {@link BufferedContextWriter#flush}.
 * </p>
 *
 * @author Sean Kerr [sean@code-box.org]
 */
public class BufferedContextWriter extends ContextWriter {
    /** The default put buffer size. */
    public static final int PUT_BUFFER_SIZE = 10000;

    /** The put buffer size. */
    private int putBufferSize = PUT_BUFFER_SIZE;

    /** The map of put operations. */
    Map<byte[],Put> puts = new HashMap();

    /**
     * Create a new BufferedContextWriter instance.
     *
     * @param context The input/output context.
     */
    public BufferedContextWriter (TaskInputOutputContext context) {
        super(context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush ()
    throws InterruptedException, IOException {
        getContext().setStatus("Flushing " + puts.size() + " puts");

        ImmutableBytesWritable tableBytes = new ImmutableBytesWritable(Bytes.toBytes(getTableName()));

        for (Put put : puts.values()) {
            if (!put.isEmpty()) {
                getContext().write(tableBytes, put);
            }
        }

        puts.clear();
        setPut(null);
    }

    /**
     * Retrieve the put buffer size.
     */
    public int getPutBufferSize () {
        return putBufferSize;
    }

    /**
     * Set the put buffer size.
     *
     * @param size The size.
     */
    public BufferedContextWriter setPutBufferSize (int size) {
        assert size > 0
             : "size <= 0";

        putBufferSize = size;

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ContextWriter setRow (byte[] row)
    throws InterruptedException, IOException {
        assert row != null
             : "row == null";

        if (getPut() == null || !Arrays.equals(getRow(), row)) {
            if (puts.size() >= getPutBufferSize()) {
                flush();
            }

            getContext().setStatus("Switching row '" + Bytes.toString(row) + "'");

            if (!puts.containsKey(row)) {
                puts.put(row, new Put(row));
            }

            setPut(puts.get(row));
        }

        return this;
    }
}