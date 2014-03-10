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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * {@link BufferedTableWriter} writes <em>Put</em> operations directly to table.
 *
 * <p>
 * Data is written when the put buffer size has been reached, when the table has been changed, or when
 * {@link BufferedTableWriter#flush} is called--whichever comes first.
 * </p>
 *
 * <p>
 * <strong>Note:</strong> {@link Writer#close} calls {@link BufferedTableWriter#flush}.
 * </p>
 *
 * @author Sean Kerr [sean@code-box.org]
 */
public class BufferedTableWriter extends TableWriter {
    /** The default put buffer size. */
    public static final int PUT_BUFFER_SIZE = 50000;

    /** The put buffer size. */
    private int putBufferSize = PUT_BUFFER_SIZE;

    /** The map of put operations. */
    Map<byte[],Put> puts = new HashMap();

    /**
     * Create a new BufferedTableWriter instance.
     */
    public BufferedTableWriter () {
        super();
    }

    /**
     * Create a new BufferedTableWriter instance.
     *
     * @param configuration The HBase configuration.
     */
    public BufferedTableWriter (Configuration configuration) {
        super(configuration);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush ()
    throws InterruptedException, IOException {
        if (getTable() != null) {
            if (getContext() != null) {
                getContext().setStatus("Flushing " + puts.size() + " puts");
            }

            if (getPut().isEmpty()) {
                puts.remove(getRow());
            }

            getTable().put(new ArrayList(puts.values()));

            puts.clear();
            setPut(null);
        }
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
    public BufferedTableWriter setPutBufferSize (int size) {
        assert size > 0
             : "size <= 0";

        putBufferSize = size;

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BufferedTableWriter setRow (byte[] row)
    throws InterruptedException, IOException {
        assert row != null
             : "row == null";

        if (getPut() == null || !Arrays.equals(getRow(), row)) {
            if (getPut() != null && getPut().isEmpty()) {
                puts.remove(getRow());
            }

            if (puts.size() >= getPutBufferSize()) {
                flush();
            }

            if (getContext() != null) {
                getContext().setStatus("Switching row '" + Bytes.toString(row) + "'");
            }

            if (!puts.containsKey(row)) {
                puts.put(row, new Put(row));
            }

            setPut(puts.get(row));
        }

        return this;
    }
}