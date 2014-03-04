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

package org.simplebase.test.model;

import org.simplebase.model.Model;
import org.simplebase.model.ModelException;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Sean Kerr [sean@code-box.org]
 */
public class ModelTest {
    /** The test family and table. */
    public static final byte[] TEST_FAMILY = Bytes.toBytes("test");
    public static final byte[] TEST_ROW    = Bytes.toBytes("model_test");
    public static final byte[] TEST_TABLE  = Bytes.toBytes("simplebase_test");

    /** The model instance. */
    private static Model model;

    /** The result. */
    private static Result result;

    /** The expected exception. */
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    // -----------------------------------------------------------------------------------------------------------------
    // TESTS
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    public void constructor () throws Exception {
        assertNotEquals(null, new Model());
        assertNotEquals(null, new Model(result));
    }

    @Test
    public void findQualifiers () throws Exception {
        assertNotNull(model);

        List<byte[]> columns = model.findQualifiers(TEST_FAMILY, "boolean");

        assertEquals(4, columns.size());
        assertTrue(Arrays.equals(Bytes.toBytes("boolean1"), columns.get(0)));
        assertTrue(Arrays.equals(Bytes.toBytes("boolean2"), columns.get(1)));
        assertTrue(Arrays.equals(Bytes.toBytes("boolean_str1"), columns.get(2)));
        assertTrue(Arrays.equals(Bytes.toBytes("boolean_str2"), columns.get(3)));

        columns = model.findQualifiers(TEST_FAMILY, "double");

        assertEquals(4, columns.size());
        assertTrue(Arrays.equals(Bytes.toBytes("double1"), columns.get(0)));
        assertTrue(Arrays.equals(Bytes.toBytes("double2"), columns.get(1)));
        assertTrue(Arrays.equals(Bytes.toBytes("double_str1"), columns.get(2)));
        assertTrue(Arrays.equals(Bytes.toBytes("double_str2"), columns.get(3)));

        columns = model.findQualifiers(TEST_FAMILY, "float");

        assertEquals(4, columns.size());
        assertTrue(Arrays.equals(Bytes.toBytes("float1"), columns.get(0)));
        assertTrue(Arrays.equals(Bytes.toBytes("float2"), columns.get(1)));
        assertTrue(Arrays.equals(Bytes.toBytes("float_str1"), columns.get(2)));
        assertTrue(Arrays.equals(Bytes.toBytes("float_str2"), columns.get(3)));

        columns = model.findQualifiers(TEST_FAMILY, "int");

        assertEquals(4, columns.size());
        assertTrue(Arrays.equals(Bytes.toBytes("int1"), columns.get(0)));
        assertTrue(Arrays.equals(Bytes.toBytes("int2"), columns.get(1)));
        assertTrue(Arrays.equals(Bytes.toBytes("int_str1"), columns.get(2)));
        assertTrue(Arrays.equals(Bytes.toBytes("int_str2"), columns.get(3)));

        columns = model.findQualifiers(TEST_FAMILY, "long");

        assertEquals(4, columns.size());
        assertTrue(Arrays.equals(Bytes.toBytes("long1"), columns.get(0)));
        assertTrue(Arrays.equals(Bytes.toBytes("long2"), columns.get(1)));
        assertTrue(Arrays.equals(Bytes.toBytes("long_str1"), columns.get(2)));
        assertTrue(Arrays.equals(Bytes.toBytes("long_str2"), columns.get(3)));

        columns = model.findQualifiers(TEST_FAMILY, "short");

        assertEquals(4, columns.size());
        assertTrue(Arrays.equals(Bytes.toBytes("short1"), columns.get(0)));
        assertTrue(Arrays.equals(Bytes.toBytes("short2"), columns.get(1)));
        assertTrue(Arrays.equals(Bytes.toBytes("short_str1"), columns.get(2)));
        assertTrue(Arrays.equals(Bytes.toBytes("short_str2"), columns.get(3)));

        columns = model.findQualifiers(TEST_FAMILY, "string");

        assertEquals(2, columns.size());
        assertTrue(Arrays.equals(Bytes.toBytes("string1"), columns.get(0)));
        assertTrue(Arrays.equals(Bytes.toBytes("string2"), columns.get(1)));
    }

    @Test
    public void getBoolean () throws Exception {
        assertNotNull(model);

        assertTrue(model.getBoolean(TEST_FAMILY, Bytes.toBytes("boolean1")));
        assertFalse(model.getBoolean(TEST_FAMILY, Bytes.toBytes("boolean2")));

        assertTrue(model.getBoolean(TEST_FAMILY, Bytes.toBytes("boolean1"), false));
        assertFalse(model.getBoolean(TEST_FAMILY, Bytes.toBytes("nonexistent"), false));

        assertNull(model.getBoolean(TEST_FAMILY, Bytes.toBytes("nonexistent"), null));

        thrown.expect(ModelException.class);
        model.getBoolean(TEST_FAMILY, Bytes.toBytes("nonexistent"));
    }

    @Test
    public void getBytes () throws Exception {
        assertNotNull(model);

        assertEquals(1, Bytes.toInt(model.getBytes(TEST_FAMILY, Bytes.toBytes("int1"))));
        assertEquals(2, Bytes.toInt(model.getBytes(TEST_FAMILY, Bytes.toBytes("int2"))));

        assertEquals(1, Bytes.toInt(model.getBytes(TEST_FAMILY, Bytes.toBytes("int1"), Bytes.toBytes(1))));
        assertEquals(2, Bytes.toInt(model.getBytes(TEST_FAMILY, Bytes.toBytes("nonexistent"), Bytes.toBytes(2))));

        assertNull(model.getBytes(TEST_FAMILY, Bytes.toBytes("nonexistent"), null));

        thrown.expect(ModelException.class);
        model.getBytes(TEST_FAMILY, Bytes.toBytes("nonexistent"));
    }

    @Test
    public void getDouble () throws Exception {
        assertNotNull(model);

        assertEquals(new Double(1.0), model.getDouble(TEST_FAMILY, Bytes.toBytes("double1")));
        assertEquals(new Double(2.0), model.getDouble(TEST_FAMILY, Bytes.toBytes("double2")));

        assertEquals(new Double(1.0), model.getDouble(TEST_FAMILY, Bytes.toBytes("double1"), (double) 1.0));
        assertEquals(new Double(2.0), model.getDouble(TEST_FAMILY, Bytes.toBytes("nonexistent"), (double) 2.0));

        assertNull(model.getDouble(TEST_FAMILY, Bytes.toBytes("nonexistent"), null));

        thrown.expect(ModelException.class);
        model.getDouble(TEST_FAMILY, Bytes.toBytes("nonexistent"));
    }

    @Test
    public void getInt () throws Exception {
        assertNotNull(model);

        assertEquals(new Integer(1), model.getInt(TEST_FAMILY, Bytes.toBytes("int1")));
        assertEquals(new Integer(2), model.getInt(TEST_FAMILY, Bytes.toBytes("int2")));

        assertEquals(new Integer(1), model.getInt(TEST_FAMILY, Bytes.toBytes("int1"), (int) 1));
        assertEquals(new Integer(2), model.getInt(TEST_FAMILY, Bytes.toBytes("nonexistent"), (int) 2));

        assertNull(model.getInt(TEST_FAMILY, Bytes.toBytes("nonexistent"), null));

        thrown.expect(ModelException.class);
        model.getInt(TEST_FAMILY, Bytes.toBytes("nonexistent"));
    }

    @Test
    public void getLong () throws Exception {
        assertNotNull(model);

        assertEquals(new Long((long) 1), model.getLong(TEST_FAMILY, Bytes.toBytes("long1")));
        assertEquals(new Long((long) 2), model.getLong(TEST_FAMILY, Bytes.toBytes("long2")));

        assertEquals(new Long((long) 1), model.getLong(TEST_FAMILY, Bytes.toBytes("long1"), (long) 1));
        assertEquals(new Long((long) 2), model.getLong(TEST_FAMILY, Bytes.toBytes("nonexistent"), (long) 2));

        assertNull(model.getLong(TEST_FAMILY, Bytes.toBytes("nonexistent"), null));

        thrown.expect(ModelException.class);
        model.getLong(TEST_FAMILY, Bytes.toBytes("nonexistent"));
    }

    @Test
    public void getResult () throws Exception {
        assertEquals(result, model.getResult());
        assertNotNull(model.getResult());
    }

    @Test
    public void getShort () throws Exception {
        assertNotNull(model);

        assertEquals(new Short((short) 1), model.getShort(TEST_FAMILY, Bytes.toBytes("short1")));
        assertEquals(new Short((short) 2), model.getShort(TEST_FAMILY, Bytes.toBytes("short2")));

        assertEquals(new Short((short) 1), model.getShort(TEST_FAMILY, Bytes.toBytes("short1"), (short) 1));
        assertEquals(new Short((short) 2), model.getShort(TEST_FAMILY, Bytes.toBytes("nonexistent"), (short) 2));

        assertNull(model.getShort(TEST_FAMILY, Bytes.toBytes("nonexistent"), null));

        thrown.expect(ModelException.class);
        model.getShort(TEST_FAMILY, Bytes.toBytes("nonexistent"));
    }

    @Test
    public void getString () throws Exception {
        assertEquals("hbase", model.getString(TEST_FAMILY, Bytes.toBytes("string1")));
        assertEquals("rocks", model.getString(TEST_FAMILY, Bytes.toBytes("string2")));

        assertEquals("hbase", model.getString(TEST_FAMILY, Bytes.toBytes("string1"), "hbase"));
        assertEquals("rocks", model.getString(TEST_FAMILY, Bytes.toBytes("nonexistent"), "rocks"));

        assertNull(model.getString(TEST_FAMILY, Bytes.toBytes("nonexistent"), null));

        thrown.expect(ModelException.class);
        model.getString(TEST_FAMILY, Bytes.toBytes("nonexistent"));
    }

    @Test
    public void hasColumn () {
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("boolean1")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("boolean2")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("double1")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("double2")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("float1")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("float2")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("int1")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("int2")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("long1")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("long2")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("short1")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("short2")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("string1")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("string2")));
        assertFalse(model.hasColumn(TEST_FAMILY, Bytes.toBytes("nonexistent")));
    }

    @Test
    public void hasColumns () {
        assertTrue(model.hasColumns(TEST_FAMILY, Bytes.toBytes("boolean1"),
                                                 Bytes.toBytes("boolean2"),
                                                 Bytes.toBytes("double1"),
                                                 Bytes.toBytes("double2"),
                                                 Bytes.toBytes("float1"),
                                                 Bytes.toBytes("float2"),
                                                 Bytes.toBytes("int1"),
                                                 Bytes.toBytes("int2"),
                                                 Bytes.toBytes("long1"),
                                                 Bytes.toBytes("long2"),
                                                 Bytes.toBytes("short1"),
                                                 Bytes.toBytes("short2"),
                                                 Bytes.toBytes("string1"),
                                                 Bytes.toBytes("string2")));

        assertFalse(model.hasColumns(TEST_FAMILY, Bytes.toBytes("boolean1"),
                                                  Bytes.toBytes("boolean2"),
                                                  Bytes.toBytes("double1"),
                                                  Bytes.toBytes("double2"),
                                                  Bytes.toBytes("float1"),
                                                  Bytes.toBytes("float2"),
                                                  Bytes.toBytes("int1"),
                                                  Bytes.toBytes("int2"),
                                                  Bytes.toBytes("long1"),
                                                  Bytes.toBytes("long2"),
                                                  Bytes.toBytes("short1"),
                                                  Bytes.toBytes("short2"),
                                                  Bytes.toBytes("string1"),
                                                  Bytes.toBytes("string2"),
                                                  Bytes.toBytes("nonexistent")));
    }

    @Test
    public void hasResult () {
        assertTrue(model.hasResult());
    }

    @Test
    public void isDoubleGreater () throws Exception {
        assertFalse(model.isDoubleGreater(TEST_FAMILY, Bytes.toBytes("double1"), Bytes.toBytes("double2")));
        assertTrue(model.isDoubleGreater(TEST_FAMILY, Bytes.toBytes("double2"), Bytes.toBytes("double1")));
    }

    @Test
    public void isDoubleLesser () throws Exception {
        assertTrue(model.isDoubleLesser(TEST_FAMILY, Bytes.toBytes("double1"), Bytes.toBytes("double2")));
        assertFalse(model.isDoubleLesser(TEST_FAMILY, Bytes.toBytes("double2"), Bytes.toBytes("double1")));
    }

    @Test
    public void isDoubleStrGreater () throws Exception {
        assertFalse(model.isDoubleStrGreater(TEST_FAMILY, Bytes.toBytes("double_str1"), Bytes.toBytes("double_str2")));
        assertTrue(model.isDoubleStrGreater(TEST_FAMILY, Bytes.toBytes("double_str2"), Bytes.toBytes("double_str1")));
    }

    @Test
    public void isDoubleStrLesser () throws Exception {
        assertTrue(model.isDoubleStrLesser(TEST_FAMILY, Bytes.toBytes("double_str1"), Bytes.toBytes("double_str2")));
        assertFalse(model.isDoubleStrLesser(TEST_FAMILY, Bytes.toBytes("double_str2"), Bytes.toBytes("double_str1")));
    }

    @Test
    public void isEqual () throws Exception {
        assertTrue(model.isEqual(TEST_FAMILY, Bytes.toBytes("boolean1"), Bytes.toBytes("boolean1")));
        assertTrue(model.isEqual(TEST_FAMILY, Bytes.toBytes("double1"), Bytes.toBytes("double1")));
        assertTrue(model.isEqual(TEST_FAMILY, Bytes.toBytes("float1"), Bytes.toBytes("float1")));
        assertTrue(model.isEqual(TEST_FAMILY, Bytes.toBytes("int1"), Bytes.toBytes("int1")));
        assertTrue(model.isEqual(TEST_FAMILY, Bytes.toBytes("long1"), Bytes.toBytes("long1")));
        assertTrue(model.isEqual(TEST_FAMILY, Bytes.toBytes("short1"), Bytes.toBytes("short1")));
        assertTrue(model.isEqual(TEST_FAMILY, Bytes.toBytes("string1"), Bytes.toBytes("string1")));

        assertFalse(model.isEqual(TEST_FAMILY, Bytes.toBytes("boolean1"), Bytes.toBytes("boolean2")));
        assertFalse(model.isEqual(TEST_FAMILY, Bytes.toBytes("double1"), Bytes.toBytes("double2")));
        assertFalse(model.isEqual(TEST_FAMILY, Bytes.toBytes("float1"), Bytes.toBytes("float2")));
        assertFalse(model.isEqual(TEST_FAMILY, Bytes.toBytes("int1"), Bytes.toBytes("int2")));
        assertFalse(model.isEqual(TEST_FAMILY, Bytes.toBytes("long1"), Bytes.toBytes("long2")));
        assertFalse(model.isEqual(TEST_FAMILY, Bytes.toBytes("short1"), Bytes.toBytes("short2")));
        assertFalse(model.isEqual(TEST_FAMILY, Bytes.toBytes("string1"), Bytes.toBytes("string2")));
    }

    @Test
    public void isFloatGreater () throws Exception {
        assertFalse(model.isFloatGreater(TEST_FAMILY, Bytes.toBytes("float1"), Bytes.toBytes("float2")));
        assertTrue(model.isFloatGreater(TEST_FAMILY, Bytes.toBytes("float2"), Bytes.toBytes("float1")));
    }

    @Test
    public void isFloatLesser () throws Exception {
        assertTrue(model.isFloatLesser(TEST_FAMILY, Bytes.toBytes("float1"), Bytes.toBytes("float2")));
        assertFalse(model.isFloatLesser(TEST_FAMILY, Bytes.toBytes("float2"), Bytes.toBytes("float1")));
    }

    @Test
    public void isFloatStrGreater () throws Exception {
        assertFalse(model.isFloatStrGreater(TEST_FAMILY, Bytes.toBytes("float_str1"), Bytes.toBytes("float_str2")));
        assertTrue(model.isFloatStrGreater(TEST_FAMILY, Bytes.toBytes("float_str2"), Bytes.toBytes("float_str1")));
    }

    @Test
    public void isFloatStrLesser () throws Exception {
        assertTrue(model.isFloatStrLesser(TEST_FAMILY, Bytes.toBytes("float_str1"), Bytes.toBytes("float_str2")));
        assertFalse(model.isFloatStrLesser(TEST_FAMILY, Bytes.toBytes("float_str2"), Bytes.toBytes("float_str1")));
    }

    @Test
    public void isIntGreater () throws Exception {
        assertFalse(model.isIntGreater(TEST_FAMILY, Bytes.toBytes("int1"), Bytes.toBytes("int2")));
        assertTrue(model.isIntGreater(TEST_FAMILY, Bytes.toBytes("int2"), Bytes.toBytes("int1")));
    }

    @Test
    public void isIntLesser () throws Exception {
        assertTrue(model.isIntLesser(TEST_FAMILY, Bytes.toBytes("int1"), Bytes.toBytes("int2")));
        assertFalse(model.isIntLesser(TEST_FAMILY, Bytes.toBytes("int2"), Bytes.toBytes("int1")));
    }

    @Test
    public void isIntStrGreater () throws Exception {
        assertFalse(model.isIntStrGreater(TEST_FAMILY, Bytes.toBytes("int_str1"), Bytes.toBytes("int_str2")));
        assertTrue(model.isIntStrGreater(TEST_FAMILY, Bytes.toBytes("int_str2"), Bytes.toBytes("int_str1")));
    }

    @Test
    public void isIntStrLesser () throws Exception {
        assertTrue(model.isIntStrLesser(TEST_FAMILY, Bytes.toBytes("int_str1"), Bytes.toBytes("int_str2")));
        assertFalse(model.isIntStrLesser(TEST_FAMILY, Bytes.toBytes("int_str2"), Bytes.toBytes("int_str1")));
    }

    @Test
    public void isLongGreater () throws Exception {
        assertFalse(model.isLongGreater(TEST_FAMILY, Bytes.toBytes("long1"), Bytes.toBytes("long2")));
        assertTrue(model.isLongGreater(TEST_FAMILY, Bytes.toBytes("long2"), Bytes.toBytes("long1")));
    }

    @Test
    public void isLongLesser () throws Exception {
        assertTrue(model.isLongLesser(TEST_FAMILY, Bytes.toBytes("long1"), Bytes.toBytes("long2")));
        assertFalse(model.isLongLesser(TEST_FAMILY, Bytes.toBytes("long2"), Bytes.toBytes("long1")));
    }

    @Test
    public void isLongStrGreater () throws Exception {
        assertFalse(model.isLongStrGreater(TEST_FAMILY, Bytes.toBytes("long_str1"), Bytes.toBytes("long_str2")));
        assertTrue(model.isLongStrGreater(TEST_FAMILY, Bytes.toBytes("long_str2"), Bytes.toBytes("long_str1")));
    }

    @Test
    public void isLongStrLesser () throws Exception {
        assertTrue(model.isLongStrLesser(TEST_FAMILY, Bytes.toBytes("long_str1"), Bytes.toBytes("long_str2")));
        assertFalse(model.isLongStrLesser(TEST_FAMILY, Bytes.toBytes("long_str2"), Bytes.toBytes("long_str1")));
    }

    @Test
    public void isShortGreater () throws Exception {
        assertFalse(model.isShortGreater(TEST_FAMILY, Bytes.toBytes("short1"), Bytes.toBytes("short2")));
        assertTrue(model.isShortGreater(TEST_FAMILY, Bytes.toBytes("short2"), Bytes.toBytes("short1")));
    }

    @Test
    public void isShortLesser () throws Exception {
        assertTrue(model.isShortLesser(TEST_FAMILY, Bytes.toBytes("short1"), Bytes.toBytes("short2")));
        assertFalse(model.isShortLesser(TEST_FAMILY, Bytes.toBytes("short2"), Bytes.toBytes("short1")));
    }

    @Test
    public void isShortStrGreater () throws Exception {
        assertFalse(model.isShortStrGreater(TEST_FAMILY, Bytes.toBytes("short_str1"), Bytes.toBytes("short_str2")));
        assertTrue(model.isShortStrGreater(TEST_FAMILY, Bytes.toBytes("short_str2"), Bytes.toBytes("short_str1")));
    }

    @Test
    public void isShortStrLesser () throws Exception {
        assertTrue(model.isShortStrLesser(TEST_FAMILY, Bytes.toBytes("short_str1"), Bytes.toBytes("short_str2")));
        assertFalse(model.isShortStrLesser(TEST_FAMILY, Bytes.toBytes("short_str2"), Bytes.toBytes("short_str1")));
    }

    @Test
    public void parseBoolean () throws Exception {
        assertNotNull(model);

        assertTrue(model.parseBoolean(TEST_FAMILY, Bytes.toBytes("boolean_str1")));
        assertFalse(model.parseBoolean(TEST_FAMILY, Bytes.toBytes("boolean_str2")));

        assertTrue(model.parseBoolean(TEST_FAMILY, Bytes.toBytes("boolean_str1"), false));
        assertFalse(model.parseBoolean(TEST_FAMILY, Bytes.toBytes("nonexistent"), false));

        assertNull(model.parseBoolean(TEST_FAMILY, Bytes.toBytes("nonexistent"), null));

        thrown.expect(ModelException.class);
        model.parseBoolean(TEST_FAMILY, Bytes.toBytes("nonexistent"));
    }

    @Test
    public void parseDouble () throws Exception {
        assertNotNull(model);

        assertEquals(new Double(1.0), model.parseDouble(TEST_FAMILY, Bytes.toBytes("double_str1")));
        assertEquals(new Double(2.0), model.parseDouble(TEST_FAMILY, Bytes.toBytes("double_str2")));

        assertEquals(new Double(1.0), model.parseDouble(TEST_FAMILY, Bytes.toBytes("double_str1"), (double) 1.0));
        assertEquals(new Double(2.0), model.parseDouble(TEST_FAMILY, Bytes.toBytes("nonexistent"), (double) 2.0));

        assertNull(model.parseDouble(TEST_FAMILY, Bytes.toBytes("nonexistent"), null));

        thrown.expect(ModelException.class);
        model.parseDouble(TEST_FAMILY, Bytes.toBytes("nonexistent"));
    }

    @Test
    public void parseInt () throws Exception {
        assertNotNull(model);

        assertEquals(new Integer(1), model.parseInt(TEST_FAMILY, Bytes.toBytes("int_str1")));
        assertEquals(new Integer(2), model.parseInt(TEST_FAMILY, Bytes.toBytes("int_str2")));

        assertEquals(new Integer(1), model.parseInt(TEST_FAMILY, Bytes.toBytes("int_str1"), (int) 1));
        assertEquals(new Integer(2), model.parseInt(TEST_FAMILY, Bytes.toBytes("nonexistent"), (int) 2));

        assertNull(model.parseInt(TEST_FAMILY, Bytes.toBytes("nonexistent"), null));

        thrown.expect(ModelException.class);
        model.parseInt(TEST_FAMILY, Bytes.toBytes("nonexistent"));
    }

    @Test
    public void parseLong () throws Exception {
        assertNotNull(model);

        assertEquals(new Long((long) 1), model.parseLong(TEST_FAMILY, Bytes.toBytes("long_str1")));
        assertEquals(new Long((long) 2), model.parseLong(TEST_FAMILY, Bytes.toBytes("long_str2")));

        assertEquals(new Long((long) 1), model.parseLong(TEST_FAMILY, Bytes.toBytes("long_str1"), (long) 1));
        assertEquals(new Long((long) 2), model.parseLong(TEST_FAMILY, Bytes.toBytes("nonexistent"), (long) 2));

        assertNull(model.parseLong(TEST_FAMILY, Bytes.toBytes("nonexistent"), null));

        thrown.expect(ModelException.class);
        model.parseLong(TEST_FAMILY, Bytes.toBytes("nonexistent"));
    }

    @Test
    public void parseShort () throws Exception {
        assertNotNull(model);

        assertEquals(new Short((short) 1), model.parseShort(TEST_FAMILY, Bytes.toBytes("short_str1")));
        assertEquals(new Short((short) 2), model.parseShort(TEST_FAMILY, Bytes.toBytes("short_str2")));

        assertEquals(new Short((short) 1), model.parseShort(TEST_FAMILY, Bytes.toBytes("short_str1"), (short) 1));
        assertEquals(new Short((short) 2), model.parseShort(TEST_FAMILY, Bytes.toBytes("nonexistent"), (short) 2));

        assertNull(model.parseShort(TEST_FAMILY, Bytes.toBytes("nonexistent"), null));

        thrown.expect(ModelException.class);
        model.parseShort(TEST_FAMILY, Bytes.toBytes("nonexistent"));
    }

    @Test
    public void setResult () {
        assertEquals(model, model.setResult(null));
        assertNull(model.getResult());
        assertEquals(result, model.setResult(result).getResult());
    }

    // -----------------------------------------------------------------------------------------------------------------
    // HELPERS
    // -----------------------------------------------------------------------------------------------------------------

    @BeforeClass
    public static void setup ()
    throws Exception {
        Configuration config = HBaseConfiguration.create();

        if (System.getenv("HB_MASTER") != null) {
            config.set("hbase.master", System.getenv("HB_MASTER"));
        }

        if (System.getenv("ZK_QUORUM") != null) {
            config.set("hbase.zookeeper.quorum", System.getenv("ZK_QUORUM"));

            if (System.getenv("ZK_PORT") != null) {
                config.set("hbase.zookeeper.property.clientPort", System.getenv("ZK_PORT"));
            }
        }

        HTable table = new HTable(config, TEST_TABLE);

        try {
            result = table.get(new Get(TEST_ROW));

            if (result != null) {
                model = new Model(result);

                return;
            }

            throw new Exception("Missing HBase row 'model_test'");
        } finally {
            table.close();
        }
    }
}