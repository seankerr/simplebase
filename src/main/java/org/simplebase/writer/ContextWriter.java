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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * {@link ContextWriter} writes <em>Put</em> operations to a mapreduce context.
 *
 * <p>
 * Data is written when the row changes, table changes, or when {@link ContextWriter#flush} is called--whichever comes
 * first.
 * </p>
 *
 * <p>
 * <strong>Note:</strong> {@link Writer#close} calls {@link ContextWriter#flush}.
 * </p>
 *
 * @author Sean Kerr [sean@code-box.org]
 */
public class ContextWriter extends Writer {
    /** The context. */
    private TaskInputOutputContext context;

    /** The currenty active table. */
    private String table;

    /**
     * Create a new ContextWriter instance.
     *
     * @param context The input/output context.
     */
    public ContextWriter (TaskInputOutputContext context) {
        assert context != null
             : "context == null";

        this.context = context;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close ()
    throws InterruptedException, IOException {
        flush();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush ()
    throws InterruptedException, IOException {
        if (getPut() != null && !getPut().isEmpty()) {
            context.write(new ImmutableBytesWritable(Bytes.toBytes(table)), getPut());

            setPut(null);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Configuration getConfiguration () {
        assert context != null
             : "context == null";

        return context.getConfiguration();
    }

    /**
     * Retrieve the context.
     */
    public TaskInputOutputContext getContext () {
        return context;
    }

    /**
     * Retrieve the currently active table name.
     */
    @Override
    public String getTableName () {
        return table;
    }

    /**
     * Set the context.
     *
     * @param context The context.
     */
    public ContextWriter setContext (TaskInputOutputContext context) {
        assert context != null
             : "context == null";

        this.context = context;

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
            flush();

            setPut(new Put(row));
        }

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ContextWriter setTableName (String table)
    throws InterruptedException, IOException {
        assert table != null
             : "table == null";

        if (this.table == null || !this.table.equals(table)) {
            flush();

            this.table = table;
        }

        return this;
    }
}