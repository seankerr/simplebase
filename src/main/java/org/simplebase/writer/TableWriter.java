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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * {@link TableWriter} writes <em>Put</em> operations directly to table.
 *
 * <p>
 * Data is written when the row changes, table changes, or when {@link TableWriter#flush} is called--whichever comes
 * first.
 * </p>
 *
 * <p>
 * <strong>Note:</strong> {@link Writer#close} calls {@link TableWriter#flush}.
 * </p>
 *
 * @author Sean Kerr [sean@code-box.org]
 */
public class TableWriter extends Writer {
    /** The default table write buffer size. */
    public static final int TABLE_WRITE_BUFFER_SIZE = 1024 * 1024 * 16;

    /** The configuration. */
    private Configuration configuration;

    /** The currenty active table. */
    private HTableInterface table;

    /** The map of tables. */
    private Map<String,HTableInterface> tables = new HashMap();

    /** The table write buffer size. */
    private int tableWriteBufferSize = TABLE_WRITE_BUFFER_SIZE;

    /**
     * Create a new TableWriter instance.
     */
    public TableWriter () {
        setConfiguration(HBaseConfiguration.create());
    }

    /**
     * Create a new TableWriter instance.
     *
     * @param configuration The HBase configuration.
     */
    public TableWriter (Configuration configuration) {
        assert configuration != null
             : "configuration == null";

        setConfiguration(configuration);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close ()
    throws InterruptedException, IOException {
        flush();

        for (HTableInterface table : tables.values()) {
            table.flushCommits();
            table.close();
        }

        tables.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush ()
    throws InterruptedException, IOException {
        if (getPut() != null && !getPut().isEmpty()) {
            table.put(getPut());

            setPut(null);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Configuration getConfiguration () {
        return configuration;
    }

    /**
     * Retrieve the currently active table.
     */
    protected HTableInterface getTable () {
        return table;
    }

    /**
     * Retrieve the map of open tables.
     */
    protected Map<String,HTableInterface> getTables () {
        return tables;
    }

    /**
     * Retrieve the currently active table name.
     */
    @Override
    public String getTableName () {
        assert table != null
             : "table == null";

        return Bytes.toString(table.getTableName());
    }

    /**
     * Retrieve the table write buffer size.
     */
    public int getTableWriteBufferSize () {
        return tableWriteBufferSize;
    }

    /**
     * Set the HBase configuration.
     *
     * @param configuration The configuration.
     */
    public TableWriter setConfiguration (Configuration configuration) {
        this.configuration = configuration;

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableWriter setRow (byte[] row)
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
    public TableWriter setTableName (String table)
    throws InterruptedException, IOException {
        assert table != null
             : "table == null";

        if (this.table == null || !Arrays.equals(this.table.getTableName(), Bytes.toBytes(table))) {
            flush();

            if (!tables.containsKey(table)) {
                HTableInterface _table = new HTable(getConfiguration(), table);

                _table.setWriteBufferSize(getTableWriteBufferSize());
                tables.put(table, _table);
            }

            this.table = tables.get(table);
        }

        return this;
    }

    /**
     * Set the table write buffer size.
     *
     * @param size The size.
     */
    public TableWriter setTableWriteBufferSize (int size) {
        assert size > 0
             : "size <= 0";

        this.tableWriteBufferSize = size;

        return this;
    }
}