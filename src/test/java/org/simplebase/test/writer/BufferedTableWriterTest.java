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
import org.simplebase.writer.BufferedTableWriter;

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
public class BufferedTableWriterTest extends TableWriterTest {
    // -----------------------------------------------------------------------------------------------------------------
    // TESTS
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    public void getPutBufferSize ()
    throws Exception {
        init();

        BufferedTableWriter writer = (BufferedTableWriter) this.writer;

        assertTrue(writer.PUT_BUFFER_SIZE == writer.getPutBufferSize());
        assertEquals(writer, writer.setPutBufferSize(500));
        assertTrue(500 == writer.getPutBufferSize());
    }

    @Test
    public void setRowTest ()
    throws Exception {
        // test 1
        init();

        BufferedTableWriter writer = (BufferedTableWriter) this.writer;

        assertFalse(hasRow(ROW1));

        writer.setPutBufferSize(1);
        writer.setRow(ROW1);

        assertTrue(Arrays.equals(ROW1, writer.getPut().getRow()));

        writer.writeString(QUALIFIER, "simplebase");
        writer.setRow(ROW2);

        assertTrue(hasRow(ROW1));
        assertFalse(hasRow(ROW2));
        assertTrue(Arrays.equals(ROW2, writer.getPut().getRow()));

        writer.writeString(QUALIFIER, "simplebase");
        writer.setRow(ROW1);

        assertTrue(hasRow(ROW2));

        switchModel(ROW1);

        assertEquals("simplebase", model.getString(QUALIFIER));

        switchModel(ROW2);

        assertEquals("simplebase", model.getString(QUALIFIER));

        // test 2
        init();

        writer = (BufferedTableWriter) this.writer;

        assertFalse(hasRow(ROW1));

        writer.setPutBufferSize(2);
        writer.setRow(ROW1);

        assertTrue(Arrays.equals(ROW1, writer.getPut().getRow()));

        writer.writeString(QUALIFIER, "simplebase");
        writer.setRow(ROW2);

        assertFalse(hasRow(ROW1));
        assertFalse(hasRow(ROW2));
        assertFalse(hasRow(ROW3));
        assertTrue(Arrays.equals(ROW2, writer.getPut().getRow()));

        writer.writeString(QUALIFIER, "simplebase");
        writer.setRow(ROW3);

        assertTrue(hasRow(ROW1));
        assertTrue(hasRow(ROW2));
        assertFalse(hasRow(ROW3));

        switchModel(ROW1);

        assertEquals("simplebase", model.getString(QUALIFIER));

        switchModel(ROW2);

        assertEquals("simplebase", model.getString(QUALIFIER));
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

        deleteRow(ROW1);
        deleteRow(ROW2);
        deleteRow(ROW3);

        if (writer != null) {
            writer.close();

            writer = null;
        }

        setWriter(new BufferedTableWriter(config).setColumnFamily(FAMILY1)
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