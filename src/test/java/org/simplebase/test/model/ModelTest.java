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
    /** A nonexistent value. */
    private static final byte[] NONEXISTENT = Bytes.toBytes("nonexistent");

    /** The test family and table. */
    private static final byte[] TEST_FAMILY = Bytes.toBytes("test");
    private static final byte[] TEST_ROW    = Bytes.toBytes("model_test");
    private static final byte[] TEST_TABLE  = Bytes.toBytes("simplebase_test");

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

        thrown.expect(ModelException.class);
        model.getBoolean(TEST_FAMILY, NONEXISTENT);
    }

    @Test
    public void getBooleanD () {
        assertTrue(model.getBooleanD(TEST_FAMILY, Bytes.toBytes("boolean1"), false));
        assertFalse(model.getBooleanD(TEST_FAMILY, NONEXISTENT, false));

        assertNull(model.getBooleanD(TEST_FAMILY, NONEXISTENT, null));
    }

    @Test
    public void getBytes () throws Exception {
        assertNotNull(model);

        assertEquals(1, Bytes.toInt(model.getBytes(TEST_FAMILY, Bytes.toBytes("int1"))));
        assertEquals(2, Bytes.toInt(model.getBytes(TEST_FAMILY, Bytes.toBytes("int2"))));

        thrown.expect(ModelException.class);
        model.getBytes(TEST_FAMILY, NONEXISTENT);
    }

    @Test
    public void getBytesD () {
        assertEquals(1, Bytes.toInt(model.getBytesD(TEST_FAMILY, Bytes.toBytes("int1"), Bytes.toBytes(1))));
        assertEquals(2, Bytes.toInt(model.getBytesD(TEST_FAMILY, NONEXISTENT, Bytes.toBytes(2))));

        assertNull(model.getBytesD(TEST_FAMILY, NONEXISTENT, null));
    }

    @Test
    public void getDouble () throws Exception {
        assertNotNull(model);

        assertEquals(new Double(1.0), model.getDouble(TEST_FAMILY, Bytes.toBytes("double1")));
        assertEquals(new Double(2.0), model.getDouble(TEST_FAMILY, Bytes.toBytes("double2")));

        thrown.expect(ModelException.class);
        model.getDouble(TEST_FAMILY, NONEXISTENT);
    }

    @Test
    public void getDoubleD () throws Exception {
        assertEquals(new Double(1.0), model.getDoubleD(TEST_FAMILY, Bytes.toBytes("double1"), (double) 1.0));
        assertEquals(new Double(2.0), model.getDoubleD(TEST_FAMILY, NONEXISTENT, (double) 2.0));

        assertNull(model.getDoubleD(TEST_FAMILY, NONEXISTENT, null));
    }

    @Test
    public void getFamilies () {
        List<byte[]> families = model.getFamilies();

        assertNotNull(families);
        assertEquals(1, families.size());
        assertTrue(Arrays.equals(TEST_FAMILY, families.get(0)));
    }

    @Test
    public void getFloat () throws Exception {
        assertNotNull(model);

        assertEquals(new Float(1.0), model.getFloat(TEST_FAMILY, Bytes.toBytes("float1")));
        assertEquals(new Float(2.0), model.getFloat(TEST_FAMILY, Bytes.toBytes("float2")));

        thrown.expect(ModelException.class);
        model.getFloat(TEST_FAMILY, NONEXISTENT);
    }

    @Test
    public void getFloatD () {
        assertEquals(new Float(1.0), model.getFloatD(TEST_FAMILY, Bytes.toBytes("float1"), (float) 1.0));
        assertEquals(new Float(2.0), model.getFloatD(TEST_FAMILY, NONEXISTENT, (float) 2.0));

        assertNull(model.getFloatD(TEST_FAMILY, NONEXISTENT, null));
    }

    @Test
    public void getInt () throws Exception {
        assertNotNull(model);

        assertEquals(new Integer(1), model.getInt(TEST_FAMILY, Bytes.toBytes("int1")));
        assertEquals(new Integer(2), model.getInt(TEST_FAMILY, Bytes.toBytes("int2")));

        thrown.expect(ModelException.class);
        model.getInt(TEST_FAMILY, NONEXISTENT);
    }

    @Test
    public void getIntD () {
        assertEquals(new Integer(1), model.getIntD(TEST_FAMILY, Bytes.toBytes("int1"), (int) 1));
        assertEquals(new Integer(2), model.getIntD(TEST_FAMILY, NONEXISTENT, (int) 2));

        assertNull(model.getIntD(TEST_FAMILY, NONEXISTENT, null));
    }

    @Test
    public void getLong () throws Exception {
        assertNotNull(model);

        assertEquals(new Long((long) 1), model.getLong(TEST_FAMILY, Bytes.toBytes("long1")));
        assertEquals(new Long((long) 2), model.getLong(TEST_FAMILY, Bytes.toBytes("long2")));

        thrown.expect(ModelException.class);
        model.getLong(TEST_FAMILY, NONEXISTENT);
    }

    @Test
    public void getLongD () {
        assertEquals(new Long((long) 1), model.getLongD(TEST_FAMILY, Bytes.toBytes("long1"), (long) 1));
        assertEquals(new Long((long) 2), model.getLongD(TEST_FAMILY, NONEXISTENT, (long) 2));

        assertNull(model.getLongD(TEST_FAMILY, NONEXISTENT, null));
    }

    @Test
    public void getQualifiers () {
        List<byte[]> qualifiers = model.getQualifiers(TEST_FAMILY);

        assertNotNull(qualifiers);
        assertEquals(26, qualifiers.size());
        assertTrue(Arrays.equals(qualifiers.get(0), Bytes.toBytes("boolean1")));
        assertTrue(Arrays.equals(qualifiers.get(1), Bytes.toBytes("boolean2")));
        assertTrue(Arrays.equals(qualifiers.get(2), Bytes.toBytes("boolean_str1")));
        assertTrue(Arrays.equals(qualifiers.get(3), Bytes.toBytes("boolean_str2")));
        assertTrue(Arrays.equals(qualifiers.get(4), Bytes.toBytes("double1")));
        assertTrue(Arrays.equals(qualifiers.get(5), Bytes.toBytes("double2")));
        assertTrue(Arrays.equals(qualifiers.get(6), Bytes.toBytes("double_str1")));
        assertTrue(Arrays.equals(qualifiers.get(7), Bytes.toBytes("double_str2")));
        assertTrue(Arrays.equals(qualifiers.get(8), Bytes.toBytes("float1")));
        assertTrue(Arrays.equals(qualifiers.get(9), Bytes.toBytes("float2")));
        assertTrue(Arrays.equals(qualifiers.get(10), Bytes.toBytes("float_str1")));
        assertTrue(Arrays.equals(qualifiers.get(11), Bytes.toBytes("float_str2")));
        assertTrue(Arrays.equals(qualifiers.get(12), Bytes.toBytes("int1")));
        assertTrue(Arrays.equals(qualifiers.get(13), Bytes.toBytes("int2")));
        assertTrue(Arrays.equals(qualifiers.get(14), Bytes.toBytes("int_str1")));
        assertTrue(Arrays.equals(qualifiers.get(15), Bytes.toBytes("int_str2")));
        assertTrue(Arrays.equals(qualifiers.get(16), Bytes.toBytes("long1")));
        assertTrue(Arrays.equals(qualifiers.get(17), Bytes.toBytes("long2")));
        assertTrue(Arrays.equals(qualifiers.get(18), Bytes.toBytes("long_str1")));
        assertTrue(Arrays.equals(qualifiers.get(19), Bytes.toBytes("long_str2")));
        assertTrue(Arrays.equals(qualifiers.get(20), Bytes.toBytes("short1")));
        assertTrue(Arrays.equals(qualifiers.get(21), Bytes.toBytes("short2")));
        assertTrue(Arrays.equals(qualifiers.get(22), Bytes.toBytes("short_str1")));
        assertTrue(Arrays.equals(qualifiers.get(23), Bytes.toBytes("short_str2")));
        assertTrue(Arrays.equals(qualifiers.get(24), Bytes.toBytes("string1")));
        assertTrue(Arrays.equals(qualifiers.get(25), Bytes.toBytes("string2")));
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

        thrown.expect(ModelException.class);
        model.getShort(TEST_FAMILY, NONEXISTENT);
    }

    @Test
    public void getShortD () {
        assertEquals(new Short((short) 1), model.getShortD(TEST_FAMILY, Bytes.toBytes("short1"), (short) 1));
        assertEquals(new Short((short) 2), model.getShortD(TEST_FAMILY, NONEXISTENT, (short) 2));

        assertNull(model.getShortD(TEST_FAMILY, NONEXISTENT, null));
    }

    @Test
    public void getString () throws Exception {
        assertEquals("hbase", model.getString(TEST_FAMILY, Bytes.toBytes("string1")));
        assertEquals("rocks", model.getString(TEST_FAMILY, Bytes.toBytes("string2")));

        thrown.expect(ModelException.class);
        model.getString(TEST_FAMILY, NONEXISTENT);
    }

    @Test
    public void getStringD () {
        assertEquals("hbase", model.getStringD(TEST_FAMILY, Bytes.toBytes("string1"), "hbase"));
        assertEquals("rocks", model.getStringD(TEST_FAMILY, NONEXISTENT, "rocks"));

        assertNull(model.getStringD(TEST_FAMILY, NONEXISTENT, null));
    }

    @Test
    public void hasColumn () {
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("boolean1")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("boolean2")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("boolean_str1")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("boolean_str2")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("double1")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("double2")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("double_str1")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("double_str2")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("float1")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("float2")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("float_str1")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("float_str2")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("int1")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("int2")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("int_str1")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("int_str2")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("long1")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("long2")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("long_str1")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("long_str2")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("short1")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("short2")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("short_str1")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("short_str2")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("string1")));
        assertTrue(model.hasColumn(TEST_FAMILY, Bytes.toBytes("string2")));
        assertFalse(model.hasColumn(TEST_FAMILY, NONEXISTENT));
    }

    @Test
    public void hasFamily () {
        assertTrue(model.hasFamily(TEST_FAMILY));
        assertFalse(model.hasFamily(NONEXISTENT));
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
    public void isDoubleGreaterS () throws Exception {
        assertFalse(model.isDoubleGreaterS(TEST_FAMILY, Bytes.toBytes("double_str1"), Bytes.toBytes("double_str2")));
        assertTrue(model.isDoubleGreaterS(TEST_FAMILY, Bytes.toBytes("double_str2"), Bytes.toBytes("double_str1")));
    }

    @Test
    public void isDoubleLesserS () throws Exception {
        assertTrue(model.isDoubleLesserS(TEST_FAMILY, Bytes.toBytes("double_str1"), Bytes.toBytes("double_str2")));
        assertFalse(model.isDoubleLesserS(TEST_FAMILY, Bytes.toBytes("double_str2"), Bytes.toBytes("double_str1")));
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
    public void isFloatGreaterS () throws Exception {
        assertFalse(model.isFloatGreaterS(TEST_FAMILY, Bytes.toBytes("float_str1"), Bytes.toBytes("float_str2")));
        assertTrue(model.isFloatGreaterS(TEST_FAMILY, Bytes.toBytes("float_str2"), Bytes.toBytes("float_str1")));
    }

    @Test
    public void isFloatLesserS () throws Exception {
        assertTrue(model.isFloatLesserS(TEST_FAMILY, Bytes.toBytes("float_str1"), Bytes.toBytes("float_str2")));
        assertFalse(model.isFloatLesserS(TEST_FAMILY, Bytes.toBytes("float_str2"), Bytes.toBytes("float_str1")));
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
    public void isIntGreaterS () throws Exception {
        assertFalse(model.isIntGreaterS(TEST_FAMILY, Bytes.toBytes("int_str1"), Bytes.toBytes("int_str2")));
        assertTrue(model.isIntGreaterS(TEST_FAMILY, Bytes.toBytes("int_str2"), Bytes.toBytes("int_str1")));
    }

    @Test
    public void isIntLesserS () throws Exception {
        assertTrue(model.isIntLesserS(TEST_FAMILY, Bytes.toBytes("int_str1"), Bytes.toBytes("int_str2")));
        assertFalse(model.isIntLesserS(TEST_FAMILY, Bytes.toBytes("int_str2"), Bytes.toBytes("int_str1")));
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
    public void isLongGreaterS () throws Exception {
        assertFalse(model.isLongGreaterS(TEST_FAMILY, Bytes.toBytes("long_str1"), Bytes.toBytes("long_str2")));
        assertTrue(model.isLongGreaterS(TEST_FAMILY, Bytes.toBytes("long_str2"), Bytes.toBytes("long_str1")));
    }

    @Test
    public void isLongLesserS () throws Exception {
        assertTrue(model.isLongLesserS(TEST_FAMILY, Bytes.toBytes("long_str1"), Bytes.toBytes("long_str2")));
        assertFalse(model.isLongLesserS(TEST_FAMILY, Bytes.toBytes("long_str2"), Bytes.toBytes("long_str1")));
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
    public void isShortGreaterS () throws Exception {
        assertFalse(model.isShortGreaterS(TEST_FAMILY, Bytes.toBytes("short_str1"), Bytes.toBytes("short_str2")));
        assertTrue(model.isShortGreaterS(TEST_FAMILY, Bytes.toBytes("short_str2"), Bytes.toBytes("short_str1")));
    }

    @Test
    public void isShortLesserS () throws Exception {
        assertTrue(model.isShortLesserS(TEST_FAMILY, Bytes.toBytes("short_str1"), Bytes.toBytes("short_str2")));
        assertFalse(model.isShortLesserS(TEST_FAMILY, Bytes.toBytes("short_str2"), Bytes.toBytes("short_str1")));
    }

    @Test
    public void parseBoolean () throws Exception {
        assertNotNull(model);

        assertTrue(model.parseBoolean(TEST_FAMILY, Bytes.toBytes("boolean_str1")));
        assertFalse(model.parseBoolean(TEST_FAMILY, Bytes.toBytes("boolean_str2")));

        thrown.expect(ModelException.class);
        model.parseBoolean(TEST_FAMILY, NONEXISTENT);
    }

    @Test
    public void parseBooleanD () {
        assertTrue(model.parseBooleanD(TEST_FAMILY, Bytes.toBytes("boolean_str1"), false));
        assertFalse(model.parseBooleanD(TEST_FAMILY, NONEXISTENT, false));

        assertNull(model.parseBooleanD(TEST_FAMILY, NONEXISTENT, null));
    }

    @Test
    public void parseDouble () throws Exception {
        assertNotNull(model);

        assertEquals(new Double(1.0), model.parseDouble(TEST_FAMILY, Bytes.toBytes("double_str1")));
        assertEquals(new Double(2.0), model.parseDouble(TEST_FAMILY, Bytes.toBytes("double_str2")));

        thrown.expect(ModelException.class);
        model.parseDouble(TEST_FAMILY, NONEXISTENT);
    }

    @Test
    public void parseDoubleD () {
        assertEquals(new Double(1.0), model.parseDoubleD(TEST_FAMILY, Bytes.toBytes("double_str1"), (double) 1.0));
        assertEquals(new Double(2.0), model.parseDoubleD(TEST_FAMILY, NONEXISTENT, (double) 2.0));

        assertNull(model.parseDoubleD(TEST_FAMILY, NONEXISTENT, null));
    }

    @Test
    public void parseInt () throws Exception {
        assertNotNull(model);

        assertEquals(new Integer(1), model.parseInt(TEST_FAMILY, Bytes.toBytes("int_str1")));
        assertEquals(new Integer(2), model.parseInt(TEST_FAMILY, Bytes.toBytes("int_str2")));

        thrown.expect(ModelException.class);
        model.parseInt(TEST_FAMILY, NONEXISTENT);
    }

    @Test
    public void parseIntD () {
        assertEquals(new Integer(1), model.parseIntD(TEST_FAMILY, Bytes.toBytes("int_str1"), (int) 1));
        assertEquals(new Integer(2), model.parseIntD(TEST_FAMILY, NONEXISTENT, (int) 2));

        assertNull(model.parseIntD(TEST_FAMILY, NONEXISTENT, null));
    }

    @Test
    public void parseLong () throws Exception {
        assertNotNull(model);

        assertEquals(new Long((long) 1), model.parseLong(TEST_FAMILY, Bytes.toBytes("long_str1")));
        assertEquals(new Long((long) 2), model.parseLong(TEST_FAMILY, Bytes.toBytes("long_str2")));

        thrown.expect(ModelException.class);
        model.parseLong(TEST_FAMILY, NONEXISTENT);
    }

    @Test
    public void parseLongD () {
        assertEquals(new Long((long) 1), model.parseLongD(TEST_FAMILY, Bytes.toBytes("long_str1"), (long) 1));
        assertEquals(new Long((long) 2), model.parseLongD(TEST_FAMILY, NONEXISTENT, (long) 2));

        assertNull(model.parseLongD(TEST_FAMILY, NONEXISTENT, null));
    }

    @Test
    public void parseShort () throws Exception {
        assertNotNull(model);

        assertEquals(new Short((short) 1), model.parseShort(TEST_FAMILY, Bytes.toBytes("short_str1")));
        assertEquals(new Short((short) 2), model.parseShort(TEST_FAMILY, Bytes.toBytes("short_str2")));

        thrown.expect(ModelException.class);
        model.parseShort(TEST_FAMILY, NONEXISTENT);
    }

    @Test
    public void parseShortD () {
        assertEquals(new Short((short) 1), model.parseShortD(TEST_FAMILY, Bytes.toBytes("short_str1"), (short) 1));
        assertEquals(new Short((short) 2), model.parseShortD(TEST_FAMILY, NONEXISTENT, (short) 2));

        assertNull(model.parseShortD(TEST_FAMILY, NONEXISTENT, null));
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