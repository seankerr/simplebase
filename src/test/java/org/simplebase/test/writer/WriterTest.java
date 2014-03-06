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

package org.simplebase.test.writer;

import org.simplebase.model.Model;
import org.simplebase.test.BaseTest;
import org.simplebase.writer.Writer;

import java.util.Arrays;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Sean Kerr [sean@code-box.org]
 */
public abstract class WriterTest extends BaseTest {
    /** The test qualifier. */
    public static final byte[] QUALIFIER = Bytes.toBytes("test");

    /** The test rows. */
    public static final byte[] ROW1 = Bytes.toBytes("writer_test1");
    public static final byte[] ROW2 = Bytes.toBytes("writer_test2");
    public static final byte[] ROW3 = Bytes.toBytes("writer_test3");

    /** The model. */
    public Model model;

    /** The writer. */
    public Writer writer;

    // -----------------------------------------------------------------------------------------------------------------
    // TESTS
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    public void getColumnFamilyTest ()
    throws Exception {
        init();

        assertNotNull(writer);
        assertEquals(writer, writer.setColumnFamily(FAMILY1));
        assertTrue(Arrays.equals(FAMILY1, writer.getColumnFamily()));
    }

    @Test
    public void writeBooleanTest ()
    throws Exception {
        init();

        // true
        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeBoolean(QUALIFIER, true));
        assertFalse(hasRow(ROW1));

        flushWriter();

        assertNull(writer.getPut());

        switchModel(ROW1);

        assertTrue(model.getBoolean(QUALIFIER));

        // false
        deleteRow(ROW1);

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeBoolean(QUALIFIER, false));
        assertFalse(hasRow(ROW1));

        flushWriter();

        assertNull(writer.getPut());

        switchModel(ROW1);

        assertFalse(model.getBoolean(QUALIFIER));
    }

    @Test
    public void writeBooleanSTest ()
    throws Exception {
        init();

        // true
        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeBooleanS(QUALIFIER, true));
        assertFalse(hasRow(ROW1));

        flushWriter();

        assertNull(writer.getPut());

        switchModel(ROW1);

        assertTrue(model.parseBoolean(QUALIFIER));

        // false
        deleteRow(ROW1);

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeBooleanS(QUALIFIER, false));
        assertFalse(hasRow(ROW1));

        flushWriter();

        assertNull(writer.getPut());

        switchModel(ROW1);

        assertFalse(model.parseBoolean(QUALIFIER));
    }

    @Test
    public void writeBytesTest ()
    throws Exception {
        init();

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeBytes(QUALIFIER, QUALIFIER));
        assertFalse(hasRow(ROW1));

        flushWriter();

        assertNull(writer.getPut());

        switchModel(ROW1);

        assertTrue(Arrays.equals(QUALIFIER, model.getBytes(QUALIFIER)));
    }

    @Test
    public void writeDoubleTest ()
    throws Exception {
        init();

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeDouble(QUALIFIER, (double) 3.14));
        assertFalse(hasRow(ROW1));

        flushWriter();

        assertNull(writer.getPut());

        switchModel(ROW1);

        assertTrue((double) 3.14 == model.getDouble(QUALIFIER));
    }

    @Test
    public void writeDoubleSTest ()
    throws Exception {
        init();

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeDoubleS(QUALIFIER, (double) 3.14));
        assertFalse(hasRow(ROW1));

        flushWriter();

        assertNull(writer.getPut());

        switchModel(ROW1);

        assertTrue((double) 3.14 == model.parseDouble(QUALIFIER));
    }

    @Test
    public void writeFloatTest ()
    throws Exception {
        init();

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeFloat(QUALIFIER, (float) 3.14));
        assertFalse(hasRow(ROW1));

        flushWriter();

        assertNull(writer.getPut());

        switchModel(ROW1);

        assertTrue((float) 3.14 == model.getFloat(QUALIFIER));
    }

    @Test
    public void writeFloatSTest ()
    throws Exception {
        init();

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeFloatS(QUALIFIER, (float) 3.14));
        assertFalse(hasRow(ROW1));

        flushWriter();

        assertNull(writer.getPut());

        switchModel(ROW1);

        assertTrue((float) 3.14 == model.parseFloat(QUALIFIER));
    }

    @Test
    public void writeIntTest ()
    throws Exception {
        init();

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeInt(QUALIFIER, (int) 1));
        assertFalse(hasRow(ROW1));

        flushWriter();

        assertNull(writer.getPut());

        switchModel(ROW1);

        assertTrue((int) 1 == model.getInt(QUALIFIER));
    }

    @Test
    public void writeIntSTest ()
    throws Exception {
        init();

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeIntS(QUALIFIER, (int) 1));
        assertFalse(hasRow(ROW1));

        flushWriter();

        assertNull(writer.getPut());

        switchModel(ROW1);

        assertTrue((int) 1 == model.parseInt(QUALIFIER));
    }

    @Test
    public void writeLongTest ()
    throws Exception {
        init();

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeLong(QUALIFIER, (long) 1));
        assertFalse(hasRow(ROW1));

        flushWriter();

        assertNull(writer.getPut());

        switchModel(ROW1);

        assertTrue((long) 1 == model.getLong(QUALIFIER));
    }

    @Test
    public void writeLongSTest ()
    throws Exception {
        init();

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeLongS(QUALIFIER, (long) 1));
        assertFalse(hasRow(ROW1));

        flushWriter();

        assertNull(writer.getPut());

        switchModel(ROW1);

        assertTrue((long) 1 == model.parseLong(QUALIFIER));
    }

    @Test
    public void writeShortTest ()
    throws Exception {
        init();

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeShort(QUALIFIER, (short) 1));
        assertFalse(hasRow(ROW1));

        flushWriter();

        assertNull(writer.getPut());

        switchModel(ROW1);

        assertTrue((short) 1 == model.getShort(QUALIFIER));
    }

    @Test
    public void writeShortSTest ()
    throws Exception {
        init();

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeShortS(QUALIFIER, (short) 1));
        assertFalse(hasRow(ROW1));

        flushWriter();

        assertNull(writer.getPut());

        switchModel(ROW1);

        assertTrue((short) 1 == model.parseShort(QUALIFIER));
    }

    @Test
    public void writeStringTest ()
    throws Exception {
        init();

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeString(QUALIFIER, "simplebase"));
        assertFalse(hasRow(ROW1));

        flushWriter();

        assertNull(writer.getPut());

        switchModel(ROW1);

        assertEquals("simplebase", model.getString(QUALIFIER));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // HELPERS
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Flush the currently active writer.
     */
    public void flushWriter ()
    throws Exception {
        writer.flush();
    }

    /**
     * Initialize an individual test.
     */
    public void init ()
    throws Exception {
        switchTable(TABLE1);
        deleteRow(ROW1);
        deleteRow(ROW2);
        deleteRow(ROW3);
        switchTable(TABLE2);
        deleteRow(ROW1);
        deleteRow(ROW2);
        deleteRow(ROW3);
        switchTable(TABLE1);
    }

    /**
     * Set the writer.
     *
     * @param writer The writer.
     */
    public void setWriter (Writer writer) {
        this.writer = writer;
    }

    /**
     * Switch the currently active model.
     *
     * @param row The row.
     */
    public void switchModel (byte[] row)
    throws Exception {
        Result result = getRow(row);

        if (result == null) {
            throw new Exception("Nonexistent row: " + Bytes.toString(row));
        }

        model = new Model(result).setColumnFamily(FAMILY1);
    }
}