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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * {@link Writer} is an easy-to-use interface for writing HBase rows.
 *
 * <p>
 * Using a {@link Writer} is as simple as creating an instance, calling {@link #setTableName}, calling {@link #setRow},
 * and then calling {@link #write} for each column you choose to write.
 * </p>
 *
 * <p>
 * <strong>Note:</strong> There is no need for multiple {@link Writer} instances. You can write to multiple tables
 * and rows using a single instance. You can change the table name and row as frequently as you'd like. How and when the
 * underlying <em>Put</em> operation is written is implementation specific.
 * </p>
 *
 * @author Sean Kerr [sean@code-box.org]
 */
public abstract class Writer {
    /** The default column family. */
    private byte[] family;

    /** The currently active put operation. */
    private Put put;

    /**
     * Create a new Writer instance.
     */
    public Writer () {
    }

    /**
     * Close this writer.
     *
     * <p><strong>Note:</strong> This writer will also be flushed prior to being closed.</p>
     *
     * @throws InterruptedException If an operation is interrupted.
     * @throws IOException          If an I/O error occurs.
     */
    public abstract void close ()
    throws InterruptedException, IOException;

    /**
     * Flush this writer.
     *
     * <p>
     * <strong>Note:</strong> Once flushed, {@link #setRow} must be called again prior to writing data.
     * </p>
     *
     * @throws InterruptedException If an operation is interrupted.
     * @throws IOException          If an I/O error occurs.
     */
    public abstract void flush ()
    throws InterruptedException, IOException;

    /**
     * Retrieve the HBase configuration.
     */
    public abstract Configuration getConfiguration ();

    /**
     * Retrieve the currently active <em>Put</em> operation.
     */
    public Put getPut () {
        return put;
    }

    /**
     * Retrieve the currently active row.
     */
    public byte[] getRow () {
        assert put != null
             : "put == null";

        return put.getRow();
    }

    /**
     * Retrieve the currently active table name.
     */
    public abstract String getTableName ();

    /**
     * Set the default column family.
     *
     * @param family The column family.
     */
    public Writer setColumnFamily (byte[] family) {
        this.family = family;

        return this;
    }

    /**
     * Set the currently active <em>Put</em> operation.
     */
    protected Writer setPut (Put put) {
        this.put = put;

        return this;
    }

    /**
     * Set the currently active row.
     *
     * @param row The row.
     *
     * @throws InterruptedException If an operation is interrupted.
     * @throws IOException          If an I/O error occurs.
     */
    public abstract Writer setRow (byte[] row)
    throws InterruptedException, IOException;

    /**
     * Set the currently active row.
     *
     * @param row The row.
     *
     * @throws InterruptedException If an operation is interrupted.
     * @throws IOException          If an I/O error occurs.
     */
    public Writer setRow (String row)
    throws InterruptedException, IOException {
        assert row != null
             : "row == null";

        return setRow(Bytes.toBytes(row));
    }

    /**
     * Set the currently active table name.
     *
     * <p>
     * <strong>Note:</strong> If the new table does not match the currently active table, a call to
     *                        {@link #flush} will be made, which implies the same rules.
     * </p>
     *
     * @param table The table.
     *
     * @throws InterruptedException If an operation is interrupted.
     * @throws IOException          If an I/O error occurs.
     */
    public abstract Writer setTableName (String table)
    throws InterruptedException, IOException;

    /**
     * Write a column qualifier/value pair.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier The qualifier.
     * @param value     The value.
     *
     * @throws IOException If an I/O error occurs.
     */
    public Writer write (byte[] qualifier, byte[] value)
    throws IOException {
        return write(family, qualifier, value);
    }

    /**
     * Write a column qualifier/value pair.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     * @param value     The value.
     *
     * @throws IOException If an I/O error occurs.
     */
    public Writer write (byte[] family, byte[] qualifier, byte[] value)
    throws IOException {
        assert family != null && qualifier != null && value != null
             : "family == null || qualifer == null || value == null";

        put.add(family, qualifier, value);

        return this;
    }
}