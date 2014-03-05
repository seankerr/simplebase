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

import org.simplebase.test.BaseTest;
import org.simplebase.writer.TableWriter;

import java.util.Arrays;

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
    public static final byte[] ROW1 = Bytes.toBytes("table_writer_test1");
    public static final byte[] ROW2 = Bytes.toBytes("table_writer_test2");

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
        deleteRow(ROW1);

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeBoolean(QUALIFIER, true));
        assertFalse(hasRow(ROW1));

        writer.flush();

        assertNull(writer.getPut());

        model = getModel(ROW1).setColumnFamily(FAMILY1);

        assertTrue(model.getBoolean(FAMILY1, QUALIFIER));

        // false
        deleteRow(ROW1);

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeBoolean(QUALIFIER, false));
        assertFalse(hasRow(ROW1));

        writer.flush();

        assertNull(writer.getPut());

        model = getModel(ROW1).setColumnFamily(FAMILY1);

        assertFalse(model.getBoolean(FAMILY1, QUALIFIER));
    }

    @Test
    public void writeBooleanSTest ()
    throws Exception {
        init();

        // true
        deleteRow(ROW1);

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeBooleanS(QUALIFIER, true));
        assertFalse(hasRow(ROW1));

        writer.flush();

        assertNull(writer.getPut());

        model = getModel(ROW1).setColumnFamily(FAMILY1);

        assertTrue(model.parseBoolean(FAMILY1, QUALIFIER));

        // false
        deleteRow(ROW1);

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeBooleanS(QUALIFIER, false));
        assertFalse(hasRow(ROW1));

        writer.flush();

        assertNull(writer.getPut());

        model = getModel(ROW1).setColumnFamily(FAMILY1);

        assertFalse(model.parseBoolean(FAMILY1, QUALIFIER));
    }

    @Test
    public void writeBytesTest ()
    throws Exception {
        init();
        deleteRow(ROW1);

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeBytes(QUALIFIER, QUALIFIER));
        assertFalse(hasRow(ROW1));

        writer.flush();

        assertNull(writer.getPut());

        model = getModel(ROW1).setColumnFamily(FAMILY1);

        assertTrue(Arrays.equals(QUALIFIER, model.getBytes(FAMILY1, QUALIFIER)));
    }

    @Test
    public void writeDoubleTest ()
    throws Exception {
        init();
        deleteRow(ROW1);

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeDouble(QUALIFIER, (double) 3.14));
        assertFalse(hasRow(ROW1));

        writer.flush();

        assertNull(writer.getPut());

        model = getModel(ROW1).setColumnFamily(FAMILY1);

        assertTrue((double) 3.14 == model.getDouble(FAMILY1, QUALIFIER));
    }

    @Test
    public void writeDoubleSTest ()
    throws Exception {
        init();
        deleteRow(ROW1);

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeDoubleS(QUALIFIER, (double) 3.14));
        assertFalse(hasRow(ROW1));

        writer.flush();

        assertNull(writer.getPut());

        model = getModel(ROW1).setColumnFamily(FAMILY1);

        assertTrue((double) 3.14 == model.parseDouble(FAMILY1, QUALIFIER));
    }

    @Test
    public void writeFloatTest ()
    throws Exception {
        init();
        deleteRow(ROW1);

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeFloat(QUALIFIER, (float) 3.14));
        assertFalse(hasRow(ROW1));

        writer.flush();

        assertNull(writer.getPut());

        model = getModel(ROW1).setColumnFamily(FAMILY1);

        assertTrue((float) 3.14 == model.getFloat(FAMILY1, QUALIFIER));
    }

    @Test
    public void writeFloatSTest ()
    throws Exception {
        init();
        deleteRow(ROW1);

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeFloatS(QUALIFIER, (float) 3.14));
        assertFalse(hasRow(ROW1));

        writer.flush();

        assertNull(writer.getPut());

        model = getModel(ROW1).setColumnFamily(FAMILY1);

        assertTrue((float) 3.14 == model.parseFloat(FAMILY1, QUALIFIER));
    }

    @Test
    public void writeIntTest ()
    throws Exception {
        init();
        deleteRow(ROW1);

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeInt(QUALIFIER, (int) 1));
        assertFalse(hasRow(ROW1));

        writer.flush();

        assertNull(writer.getPut());

        model = getModel(ROW1).setColumnFamily(FAMILY1);

        assertTrue((int) 1 == model.getInt(FAMILY1, QUALIFIER));
    }

    @Test
    public void writeIntSTest ()
    throws Exception {
        init();
        deleteRow(ROW1);

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeIntS(QUALIFIER, (int) 1));
        assertFalse(hasRow(ROW1));

        writer.flush();

        assertNull(writer.getPut());

        model = getModel(ROW1).setColumnFamily(FAMILY1);

        assertTrue((int) 1 == model.parseInt(FAMILY1, QUALIFIER));
    }

    @Test
    public void writeLongTest ()
    throws Exception {
        init();
        deleteRow(ROW1);

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeLong(QUALIFIER, (long) 1));
        assertFalse(hasRow(ROW1));

        writer.flush();

        assertNull(writer.getPut());

        model = getModel(ROW1).setColumnFamily(FAMILY1);

        assertTrue((long) 1 == model.getLong(FAMILY1, QUALIFIER));
    }

    @Test
    public void writeLongSTest ()
    throws Exception {
        init();
        deleteRow(ROW1);

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeLongS(QUALIFIER, (long) 1));
        assertFalse(hasRow(ROW1));

        writer.flush();

        assertNull(writer.getPut());

        model = getModel(ROW1).setColumnFamily(FAMILY1);

        assertTrue((long) 1 == model.parseLong(FAMILY1, QUALIFIER));
    }

    @Test
    public void writeShortTest ()
    throws Exception {
        init();
        deleteRow(ROW1);

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeShort(QUALIFIER, (short) 1));
        assertFalse(hasRow(ROW1));

        writer.flush();

        assertNull(writer.getPut());

        model = getModel(ROW1).setColumnFamily(FAMILY1);

        assertTrue((short) 1 == model.getShort(FAMILY1, QUALIFIER));
    }

    @Test
    public void writeShortSTest ()
    throws Exception {
        init();
        deleteRow(ROW1);

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeShortS(QUALIFIER, (short) 1));
        assertFalse(hasRow(ROW1));

        writer.flush();

        assertNull(writer.getPut());

        model = getModel(ROW1).setColumnFamily(FAMILY1);

        assertTrue((short) 1 == model.parseShort(FAMILY1, QUALIFIER));
    }

    @Test
    public void writeStringTest ()
    throws Exception {
        init();
        deleteRow(ROW1);

        assertNull(writer.getPut());
        assertEquals(writer, writer.setRow(ROW1));
        assertNotNull(writer.getPut());
        assertEquals(writer, writer.writeString(QUALIFIER, "simplebase"));
        assertFalse(hasRow(ROW1));

        writer.flush();

        assertNull(writer.getPut());

        model = getModel(ROW1).setColumnFamily(FAMILY1);

        assertEquals("simplebase", model.getString(FAMILY1, QUALIFIER));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // HELPERS
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Initialize an individual test.
     */
    public static void init ()
    throws Exception {
        if (writer != null) {
            writer.close();

            writer = null;
        }

        writer = new TableWriter(config);
        writer.setColumnFamily(FAMILY1);
        writer.setTableName(Bytes.toString(TABLE1));

        switchTable(TABLE1);
    }

    /**
     * Setup the test environment.
     */
    public static void setup ()
    throws Exception {
        BaseTest.setup();
    }
}