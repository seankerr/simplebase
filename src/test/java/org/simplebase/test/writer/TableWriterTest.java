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

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Sean Kerr [sean@code-box.org]
 */
public class TableWriterTest extends WriterTest {
    // -----------------------------------------------------------------------------------------------------------------
    // TESTS
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    public void closeTest ()
    throws Exception {
        init();

        assertFalse(hasRow(ROW1));

        writer.setRow(ROW1);
        writer.writeString(QUALIFIER, "simplebase");
        writer.close();

        assertTrue(hasRow(ROW1));

        switchModel(ROW1);

        assertEquals("simplebase", model.getString(FAMILY1, QUALIFIER));
    }

    @Test
    public void flushTest ()
    throws Exception {
        init();

        assertFalse(hasRow(ROW1));

        writer.setRow(ROW1);
        writer.writeString(QUALIFIER, "simplebase");
        writer.flush();

        assertTrue(hasRow(ROW1));

        switchModel(ROW1);

        assertEquals("simplebase", model.getString(FAMILY1, QUALIFIER));
    }

    @Test
    public void getTableWriteBufferSizeTest ()
    throws Exception {
        init();

        assertTrue(TableWriter.TABLE_WRITE_BUFFER_SIZE == ((TableWriter) writer).getTableWriteBufferSize());
        assertEquals(writer, ((TableWriter) writer).setTableWriteBufferSize(500));
        assertTrue(500 == ((TableWriter) writer).getTableWriteBufferSize());
    }

    @Test
    public void setRowTest ()
    throws Exception {
        init();

        assertFalse(hasRow(ROW1));

        writer.setRow(ROW1);

        assertTrue(Arrays.equals(ROW1, writer.getPut().getRow()));

        writer.writeString(QUALIFIER, "simplebase");
        writer.setRow(ROW2);

        assertTrue(Arrays.equals(ROW2, writer.getPut().getRow()));

        writer.writeString(QUALIFIER, "simplebase");

        assertTrue(hasRow(ROW1));
        assertFalse(hasRow(ROW2));

        writer.setRow(ROW1);

        assertTrue(hasRow(ROW2));

        switchModel(ROW1);

        assertEquals("simplebase", model.getString(FAMILY1, QUALIFIER));

        switchModel(ROW2);

        assertEquals("simplebase", model.getString(FAMILY1, QUALIFIER));
    }

    @Test
    public void setTableNameTest ()
    throws Exception {
        init();

        assertFalse(hasRow(ROW1));
        assertFalse(hasRow(ROW2));

        switchTable(TABLE2);
        deleteRow(ROW1);
        deleteRow(ROW2);

        assertFalse(hasRow(ROW1));
        assertFalse(hasRow(ROW2));

        switchTable(TABLE1);

        writer.setTableName(Bytes.toString(TABLE1));
        writer.setRow(ROW1);

        assertTrue(Arrays.equals(ROW1, writer.getPut().getRow()));

        writer.writeString(QUALIFIER, "simplebase");

        assertFalse(hasRow(ROW1));

        writer.setTableName(Bytes.toString(TABLE2));

        assertTrue(hasRow(ROW1));

        switchTable(TABLE2);
        writer.setRow(ROW2);

        assertTrue(Arrays.equals(ROW2, writer.getPut().getRow()));

        writer.writeString(QUALIFIER, "rocks");

        assertFalse(hasRow(ROW2));

        writer.setTableName(Bytes.toString(TABLE1));

        assertTrue(hasRow(ROW2));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // HELPERS
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Initialize an individual test.
     */
    public void init ()
    throws Exception {
        super.init();

        if (writer != null) {
            writer.close();

            writer = null;
        }

        setWriter(new TableWriter(config).setColumnFamily(FAMILY1)
                                         .setTableName(Bytes.toString(TABLE1)));

        switchTable(TABLE1);
    }

    /**
     * Setup the test environment.
     */
    @BeforeClass
    public static void setup ()
    throws Exception {
        BaseTest.setup();
    }
}