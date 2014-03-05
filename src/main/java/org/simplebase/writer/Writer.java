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
 * and then writing data.
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
     * Retrieve the default column family.
     */
    public byte[] getColumnFamily () {
        return family;
    }

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
     * Write a binary boolean value.
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
    public Writer writeBoolean (byte[] qualifier, boolean value)
    throws IOException {
        return writeBoolean(family, qualifier, value);
    }

    /**
     * Write a binary boolean value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     * @param value     The value.
     *
     * @throws IOException If an I/O error occurs.
     */
    public Writer writeBoolean (byte[] family, byte[] qualifier, boolean value)
    throws IOException {
        put.add(family, qualifier, Bytes.toBytes(value));

        return this;
    }

    /**
     * Write a string boolean value.
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
    public Writer writeBooleanS (byte[] qualifier, boolean value)
    throws IOException {
        return writeBooleanS(family, qualifier, value);
    }

    /**
     * Write a string boolean value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     * @param value     The value.
     *
     * @throws IOException If an I/O error occurs.
     */
    public Writer writeBooleanS (byte[] family, byte[] qualifier, boolean value)
    throws IOException {
        put.add(family, qualifier, Bytes.toBytes(value ? "true" : "false"));

        return this;
    }

    /**
     * Write a byte array value.
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
    public Writer writeBytes (byte[] qualifier, byte[] value)
    throws IOException {
        return writeBytes(family, qualifier, value);
    }

    /**
     * Write a byte array value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     * @param value     The value.
     *
     * @throws IOException If an I/O error occurs.
     */
    public Writer writeBytes (byte[] family, byte[] qualifier, byte[] value)
    throws IOException {
        assert family != null && qualifier != null && value != null
             : "family == null || qualifer == null || value == null";

        put.add(family, qualifier, value);

        return this;
    }

    /**
     * Write a binary double value.
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
    public Writer writeDouble (byte[] qualifier, double value)
    throws IOException {
        return writeDouble(family, qualifier, value);
    }

    /**
     * Write a binary double value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     * @param value     The value.
     *
     * @throws IOException If an I/O error occurs.
     */
    public Writer writeDouble (byte[] family, byte[] qualifier, double value)
    throws IOException {
        put.add(family, qualifier, Bytes.toBytes(value));

        return this;
    }

    /**
     * Write a string double value.
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
    public Writer writeDoubleS (byte[] qualifier, double value)
    throws IOException {
        return writeDoubleS(family, qualifier, value);
    }

    /**
     * Write a string double value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     * @param value     The value.
     *
     * @throws IOException If an I/O error occurs.
     */
    public Writer writeDoubleS (byte[] family, byte[] qualifier, double value)
    throws IOException {
        put.add(family, qualifier, Bytes.toBytes(String.valueOf(value)));

        return this;
    }

    /**
     * Write a binary float value.
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
    public Writer writeFloat (byte[] qualifier, float value)
    throws IOException {
        return writeFloat(family, qualifier, value);
    }

    /**
     * Write a binary float value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     * @param value     The value.
     *
     * @throws IOException If an I/O error occurs.
     */
    public Writer writeFloat (byte[] family, byte[] qualifier, float value)
    throws IOException {
        put.add(family, qualifier, Bytes.toBytes(value));

        return this;
    }

    /**
     * Write a string float value.
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
    public Writer writeFloatS (byte[] qualifier, float value)
    throws IOException {
        return writeFloatS(family, qualifier, value);
    }

    /**
     * Write a string float value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     * @param value     The value.
     *
     * @throws IOException If an I/O error occurs.
     */
    public Writer writeFloatS (byte[] family, byte[] qualifier, float value)
    throws IOException {
        put.add(family, qualifier, Bytes.toBytes(String.valueOf(value)));

        return this;
    }

    /**
     * Write a binary int value.
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
    public Writer writeInt (byte[] qualifier, int value)
    throws IOException {
        return writeInt(family, qualifier, value);
    }

    /**
     * Write a binary int value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     * @param value     The value.
     *
     * @throws IOException If an I/O error occurs.
     */
    public Writer writeInt (byte[] family, byte[] qualifier, int value)
    throws IOException {
        put.add(family, qualifier, Bytes.toBytes(value));

        return this;
    }

    /**
     * Write a string int value.
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
    public Writer writeIntS (byte[] qualifier, int value)
    throws IOException {
        return writeIntS(family, qualifier, value);
    }

    /**
     * Write a string int value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     * @param value     The value.
     *
     * @throws IOException If an I/O error occurs.
     */
    public Writer writeIntS (byte[] family, byte[] qualifier, int value)
    throws IOException {
        put.add(family, qualifier, Bytes.toBytes(String.valueOf(value)));

        return this;
    }

    /**
     * Write a binary long value.
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
    public Writer writeLong (byte[] qualifier, long value)
    throws IOException {
        return writeLong(family, qualifier, value);
    }

    /**
     * Write a binary long value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     * @param value     The value.
     *
     * @throws IOException If an I/O error occurs.
     */
    public Writer writeLong (byte[] family, byte[] qualifier, long value)
    throws IOException {
        put.add(family, qualifier, Bytes.toBytes(value));

        return this;
    }

    /**
     * Write a string long value.
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
    public Writer writeLongS (byte[] qualifier, long value)
    throws IOException {
        return writeLongS(family, qualifier, value);
    }

    /**
     * Write a string long value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     * @param value     The value.
     *
     * @throws IOException If an I/O error occurs.
     */
    public Writer writeLongS (byte[] family, byte[] qualifier, long value)
    throws IOException {
        put.add(family, qualifier, Bytes.toBytes(String.valueOf(value)));

        return this;
    }

    /**
     * Write a binary short value.
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
    public Writer writeShort (byte[] qualifier, short value)
    throws IOException {
        return writeShort(family, qualifier, value);
    }

    /**
     * Write a binary short value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     * @param value     The value.
     *
     * @throws IOException If an I/O error occurs.
     */
    public Writer writeShort (byte[] family, byte[] qualifier, short value)
    throws IOException {
        put.add(family, qualifier, Bytes.toBytes(value));

        return this;
    }

    /**
     * Write a string short value.
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
    public Writer writeShortS (byte[] qualifier, short value)
    throws IOException {
        return writeShortS(family, qualifier, value);
    }

    /**
     * Write a string short value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     * @param value     The value.
     *
     * @throws IOException If an I/O error occurs.
     */
    public Writer writeShortS (byte[] family, byte[] qualifier, short value)
    throws IOException {
        put.add(family, qualifier, Bytes.toBytes(String.valueOf(value)));

        return this;
    }

    /**
     * Write a string value.
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
    public Writer writeString (byte[] qualifier, String value)
    throws IOException {
        return writeString(family, qualifier, value);
    }

    /**
     * Write a string value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     * @param value     The value.
     *
     * @throws IOException If an I/O error occurs.
     */
    public Writer writeString (byte[] family, byte[] qualifier, String value)
    throws IOException {
        assert family != null && qualifier != null && value != null
             : "family == null || qualifer == null || value == null";

        put.add(family, qualifier, Bytes.toBytes(value));

        return this;
    }
}