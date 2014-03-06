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
import org.simplebase.test.BaseTest;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Sean Kerr [sean@code-box.org]
 */
public class ModelTest extends BaseTest {
    /** The qualifiers. */
    public static final byte[] BOOLEAN1     = Bytes.toBytes("boolean1");
    public static final byte[] BOOLEAN2     = Bytes.toBytes("boolean2");
    public static final byte[] BOOLEAN_STR1 = Bytes.toBytes("boolean_str1");
    public static final byte[] BOOLEAN_STR2 = Bytes.toBytes("boolean_str2");

    public static final byte[] DOUBLE1     = Bytes.toBytes("double1");
    public static final byte[] DOUBLE2     = Bytes.toBytes("double2");
    public static final byte[] DOUBLE_STR1 = Bytes.toBytes("double_str1");
    public static final byte[] DOUBLE_STR2 = Bytes.toBytes("double_str2");

    public static final byte[] FLOAT1     = Bytes.toBytes("float1");
    public static final byte[] FLOAT2     = Bytes.toBytes("float2");
    public static final byte[] FLOAT_STR1 = Bytes.toBytes("float_str1");
    public static final byte[] FLOAT_STR2 = Bytes.toBytes("float_str2");

    public static final byte[] INT1     = Bytes.toBytes("int1");
    public static final byte[] INT2     = Bytes.toBytes("int2");
    public static final byte[] INT_STR1 = Bytes.toBytes("int_str1");
    public static final byte[] INT_STR2 = Bytes.toBytes("int_str2");

    public static final byte[] LONG1     = Bytes.toBytes("long1");
    public static final byte[] LONG2     = Bytes.toBytes("long2");
    public static final byte[] LONG_STR1 = Bytes.toBytes("long_str1");
    public static final byte[] LONG_STR2 = Bytes.toBytes("long_str2");

    public static final byte[] SHORT1     = Bytes.toBytes("short1");
    public static final byte[] SHORT2     = Bytes.toBytes("short2");
    public static final byte[] SHORT_STR1 = Bytes.toBytes("short_str1");
    public static final byte[] SHORT_STR2 = Bytes.toBytes("short_str2");

    public static final byte[] STRING1     = Bytes.toBytes("string1");
    public static final byte[] STRING2     = Bytes.toBytes("string2");
    public static final byte[] STRING_STR1 = Bytes.toBytes("string_str1");
    public static final byte[] STRING_STR2 = Bytes.toBytes("string_str2");

    /** The test row. */
    public static final byte[] ROW = Bytes.toBytes("model_test");

    /** The model. */
    public Model model;

    // -----------------------------------------------------------------------------------------------------------------
    // TESTS
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    public void compareDoubleTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertTrue(model.compareDouble(model.COMPARE_EQ, DOUBLE1, DOUBLE1));
        assertFalse(model.compareDouble(model.COMPARE_EQ, DOUBLE1, DOUBLE2));
        assertTrue(model.compareDouble(model.COMPARE_LT, DOUBLE1, DOUBLE2));
        assertFalse(model.compareDouble(model.COMPARE_LT, DOUBLE2, DOUBLE1));
        assertTrue(model.compareDouble(model.COMPARE_GT, DOUBLE2, DOUBLE1));
        assertFalse(model.compareDouble(model.COMPARE_GT, DOUBLE1, DOUBLE2));
    }

    @Test
    public void compareDoubleSTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertTrue(model.compareDoubleS(model.COMPARE_EQ, DOUBLE_STR1, DOUBLE_STR1));
        assertFalse(model.compareDoubleS(model.COMPARE_EQ, DOUBLE_STR1, DOUBLE_STR2));
        assertTrue(model.compareDoubleS(model.COMPARE_LT, DOUBLE_STR1, DOUBLE_STR2));
        assertFalse(model.compareDoubleS(model.COMPARE_LT, DOUBLE_STR2, DOUBLE_STR1));
        assertTrue(model.compareDoubleS(model.COMPARE_GT, DOUBLE_STR2, DOUBLE_STR1));
        assertFalse(model.compareDoubleS(model.COMPARE_GT, DOUBLE_STR1, DOUBLE_STR2));
    }

    @Test
    public void compareFloatTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertTrue(model.compareFloat(model.COMPARE_EQ, FLOAT1, FLOAT1));
        assertFalse(model.compareFloat(model.COMPARE_EQ, FLOAT1, FLOAT2));
        assertTrue(model.compareFloat(model.COMPARE_LT, FLOAT1, FLOAT2));
        assertFalse(model.compareFloat(model.COMPARE_LT, FLOAT2, FLOAT1));
        assertTrue(model.compareFloat(model.COMPARE_GT, FLOAT2, FLOAT1));
        assertFalse(model.compareFloat(model.COMPARE_GT, FLOAT1, FLOAT2));
    }

    @Test
    public void compareFloatSTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertTrue(model.compareFloatS(model.COMPARE_EQ, FLOAT_STR1, FLOAT_STR1));
        assertFalse(model.compareFloatS(model.COMPARE_EQ, FLOAT_STR1, FLOAT_STR2));
        assertTrue(model.compareFloatS(model.COMPARE_LT, FLOAT_STR1, FLOAT_STR2));
        assertFalse(model.compareFloatS(model.COMPARE_LT, FLOAT_STR2, FLOAT_STR1));
        assertTrue(model.compareFloatS(model.COMPARE_GT, FLOAT_STR2, FLOAT_STR1));
        assertFalse(model.compareFloatS(model.COMPARE_GT, FLOAT_STR1, FLOAT_STR2));
    }

    @Test
    public void compareIntTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertTrue(model.compareInt(model.COMPARE_EQ, INT1, INT1));
        assertFalse(model.compareInt(model.COMPARE_EQ, INT1, INT2));
        assertTrue(model.compareInt(model.COMPARE_LT, INT1, INT2));
        assertFalse(model.compareInt(model.COMPARE_LT, INT2, INT1));
        assertTrue(model.compareInt(model.COMPARE_GT, INT2, INT1));
        assertFalse(model.compareInt(model.COMPARE_GT, INT1, INT2));
    }

    @Test
    public void compareIntSTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertTrue(model.compareIntS(model.COMPARE_EQ, INT_STR1, INT_STR1));
        assertFalse(model.compareIntS(model.COMPARE_EQ, INT_STR1, INT_STR2));
        assertTrue(model.compareIntS(model.COMPARE_LT, INT_STR1, INT_STR2));
        assertFalse(model.compareIntS(model.COMPARE_LT, INT_STR2, INT_STR1));
        assertTrue(model.compareIntS(model.COMPARE_GT, INT_STR2, INT_STR1));
        assertFalse(model.compareIntS(model.COMPARE_GT, INT_STR1, INT_STR2));
    }

    @Test
    public void compareLongTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertTrue(model.compareLong(model.COMPARE_EQ, LONG1, LONG1));
        assertFalse(model.compareLong(model.COMPARE_EQ, LONG1, LONG2));
        assertTrue(model.compareLong(model.COMPARE_LT, LONG1, LONG2));
        assertFalse(model.compareLong(model.COMPARE_LT, LONG2, LONG1));
        assertTrue(model.compareLong(model.COMPARE_GT, LONG2, LONG1));
        assertFalse(model.compareLong(model.COMPARE_GT, LONG1, LONG2));
    }

    @Test
    public void compareLongSTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertTrue(model.compareLongS(model.COMPARE_EQ, LONG_STR1, LONG_STR1));
        assertFalse(model.compareLongS(model.COMPARE_EQ, LONG_STR1, LONG_STR2));
        assertTrue(model.compareLongS(model.COMPARE_LT, LONG_STR1, LONG_STR2));
        assertFalse(model.compareLongS(model.COMPARE_LT, LONG_STR2, LONG_STR1));
        assertTrue(model.compareLongS(model.COMPARE_GT, LONG_STR2, LONG_STR1));
        assertFalse(model.compareLongS(model.COMPARE_GT, LONG_STR1, LONG_STR2));
    }

    @Test
    public void compareShortTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertTrue(model.compareShort(model.COMPARE_EQ, SHORT1, SHORT1));
        assertFalse(model.compareShort(model.COMPARE_EQ, SHORT1, SHORT2));
        assertTrue(model.compareShort(model.COMPARE_LT, SHORT1, SHORT2));
        assertFalse(model.compareShort(model.COMPARE_LT, SHORT2, SHORT1));
        assertTrue(model.compareShort(model.COMPARE_GT, SHORT2, SHORT1));
        assertFalse(model.compareShort(model.COMPARE_GT, SHORT1, SHORT2));
    }

    @Test
    public void compareShortSTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertTrue(model.compareShortS(model.COMPARE_EQ, SHORT_STR1, SHORT_STR1));
        assertFalse(model.compareShortS(model.COMPARE_EQ, SHORT_STR1, SHORT_STR2));
        assertTrue(model.compareShortS(model.COMPARE_LT, SHORT_STR1, SHORT_STR2));
        assertFalse(model.compareShortS(model.COMPARE_LT, SHORT_STR2, SHORT_STR1));
        assertTrue(model.compareShortS(model.COMPARE_GT, SHORT_STR2, SHORT_STR1));
        assertFalse(model.compareShortS(model.COMPARE_GT, SHORT_STR1, SHORT_STR2));
    }

    @Test
    public void constructorTest ()
    throws Exception {
        init();

        Result result = getRow(ROW);

        assertNotNull(new Model());
        assertNotNull(new Model(result));
    }

    @Test
    public void findQualifiersTest ()
    throws Exception {
        init();

        assertNotNull(model);

        List<byte[]> columns = model.findQualifiers("boolean");

        assertEquals(4, columns.size());
        assertTrue(Arrays.equals(BOOLEAN1, columns.get(0)));
        assertTrue(Arrays.equals(BOOLEAN2, columns.get(1)));
        assertTrue(Arrays.equals(BOOLEAN_STR1, columns.get(2)));
        assertTrue(Arrays.equals(BOOLEAN_STR2, columns.get(3)));

        columns = model.findQualifiers("double");

        assertEquals(4, columns.size());
        assertTrue(Arrays.equals(DOUBLE1, columns.get(0)));
        assertTrue(Arrays.equals(DOUBLE2, columns.get(1)));
        assertTrue(Arrays.equals(DOUBLE_STR1, columns.get(2)));
        assertTrue(Arrays.equals(DOUBLE_STR2, columns.get(3)));

        columns = model.findQualifiers("float");

        assertEquals(4, columns.size());
        assertTrue(Arrays.equals(FLOAT1, columns.get(0)));
        assertTrue(Arrays.equals(FLOAT2, columns.get(1)));
        assertTrue(Arrays.equals(FLOAT_STR1, columns.get(2)));
        assertTrue(Arrays.equals(FLOAT_STR2, columns.get(3)));

        columns = model.findQualifiers("int");

        assertEquals(4, columns.size());
        assertTrue(Arrays.equals(INT1, columns.get(0)));
        assertTrue(Arrays.equals(INT2, columns.get(1)));
        assertTrue(Arrays.equals(INT_STR1, columns.get(2)));
        assertTrue(Arrays.equals(INT_STR2, columns.get(3)));

        columns = model.findQualifiers("long");

        assertEquals(4, columns.size());
        assertTrue(Arrays.equals(LONG1, columns.get(0)));
        assertTrue(Arrays.equals(LONG2, columns.get(1)));
        assertTrue(Arrays.equals(LONG_STR1, columns.get(2)));
        assertTrue(Arrays.equals(LONG_STR2, columns.get(3)));

        columns = model.findQualifiers("short");

        assertEquals(4, columns.size());
        assertTrue(Arrays.equals(SHORT1, columns.get(0)));
        assertTrue(Arrays.equals(SHORT2, columns.get(1)));
        assertTrue(Arrays.equals(SHORT_STR1, columns.get(2)));
        assertTrue(Arrays.equals(SHORT_STR2, columns.get(3)));

        columns = model.findQualifiers("string");

        assertEquals(2, columns.size());
        assertTrue(Arrays.equals(STRING1, columns.get(0)));
        assertTrue(Arrays.equals(STRING2, columns.get(1)));
    }

    @Test
    public void getBooleanTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertTrue(model.getBoolean(BOOLEAN1));
        assertFalse(model.getBoolean(BOOLEAN2));

        thrown.expect(ModelException.class);
        model.getBoolean(NONEXISTENT);
    }

    @Test
    public void getBooleanDTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertTrue(model.getBooleanD(BOOLEAN1, false));
        assertFalse(model.getBooleanD(NONEXISTENT, false));
        assertNull(model.getBooleanD(NONEXISTENT, null));
    }

    @Test
    public void getBytesTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertEquals(1, Bytes.toInt(model.getBytes(INT1)));
        assertEquals(2, Bytes.toInt(model.getBytes(INT2)));

        thrown.expect(ModelException.class);
        model.getBytes(NONEXISTENT);
    }

    @Test
    public void getBytesDTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertEquals(1, Bytes.toInt(model.getBytesD(INT1, Bytes.toBytes(1))));
        assertEquals(2, Bytes.toInt(model.getBytesD(NONEXISTENT, Bytes.toBytes(2))));
        assertNull(model.getBytesD(NONEXISTENT, null));
    }

    @Test
    public void getColumnFamilyTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertEquals(model, model.setColumnFamily(FAMILY1));
        assertTrue(Arrays.equals(FAMILY1, model.getColumnFamily()));
    }

    @Test
    public void getDoubleTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertEquals(new Double(1.0), model.getDouble(DOUBLE1));
        assertEquals(new Double(2.0), model.getDouble(DOUBLE2));

        thrown.expect(ModelException.class);
        model.getDouble(NONEXISTENT);
    }

    @Test
    public void getDoubleDTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertEquals(new Double(1.0), model.getDoubleD(DOUBLE1, (double) 1.0));
        assertEquals(new Double(2.0), model.getDoubleD(NONEXISTENT, (double) 2.0));
        assertNull(model.getDoubleD(NONEXISTENT, null));
    }

    @Test
    public void getFamiliesTest ()
    throws Exception {
        init();

        assertNotNull(model);

        List<byte[]> families = model.getFamilies();

        assertNotNull(families);
        assertEquals(1, families.size());
        assertTrue(Arrays.equals(FAMILY1, families.get(0)));
    }

    @Test
    public void getFloatTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertEquals(new Float(1.0), model.getFloat(FLOAT1));
        assertEquals(new Float(2.0), model.getFloat(FLOAT2));

        thrown.expect(ModelException.class);
        model.getFloat(NONEXISTENT);
    }

    @Test
    public void getFloatDTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertEquals(new Float(1.0), model.getFloatD(FLOAT1, (float) 1.0));
        assertEquals(new Float(2.0), model.getFloatD(NONEXISTENT, (float) 2.0));
        assertNull(model.getFloatD(NONEXISTENT, null));
    }

    @Test
    public void getIntTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertEquals(new Integer(1), model.getInt(INT1));
        assertEquals(new Integer(2), model.getInt(INT2));

        thrown.expect(ModelException.class);
        model.getInt(NONEXISTENT);
    }

    @Test
    public void getIntDTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertEquals(new Integer(1), model.getIntD(INT1, (int) 1));
        assertEquals(new Integer(2), model.getIntD(NONEXISTENT, (int) 2));
        assertNull(model.getIntD(NONEXISTENT, null));
    }

    @Test
    public void getLongTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertEquals(new Long((long) 1), model.getLong(LONG1));
        assertEquals(new Long((long) 2), model.getLong(LONG2));

        thrown.expect(ModelException.class);
        model.getLong(NONEXISTENT);
    }

    @Test
    public void getLongDTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertEquals(new Long((long) 1), model.getLongD(LONG1, (long) 1));
        assertEquals(new Long((long) 2), model.getLongD(NONEXISTENT, (long) 2));
        assertNull(model.getLongD(NONEXISTENT, null));
    }

    @Test
    public void getQualifiersTest ()
    throws Exception {
        init();

        assertNotNull(model);

        List<byte[]> qualifiers = model.getQualifiers(FAMILY1);

        assertNotNull(qualifiers);
        assertEquals(26, qualifiers.size());
        assertTrue(Arrays.equals(qualifiers.get(0), BOOLEAN1));
        assertTrue(Arrays.equals(qualifiers.get(1), BOOLEAN2));
        assertTrue(Arrays.equals(qualifiers.get(2), BOOLEAN_STR1));
        assertTrue(Arrays.equals(qualifiers.get(3), BOOLEAN_STR2));
        assertTrue(Arrays.equals(qualifiers.get(4), DOUBLE1));
        assertTrue(Arrays.equals(qualifiers.get(5), DOUBLE2));
        assertTrue(Arrays.equals(qualifiers.get(6), DOUBLE_STR1));
        assertTrue(Arrays.equals(qualifiers.get(7), DOUBLE_STR2));
        assertTrue(Arrays.equals(qualifiers.get(8), FLOAT1));
        assertTrue(Arrays.equals(qualifiers.get(9), FLOAT2));
        assertTrue(Arrays.equals(qualifiers.get(10), FLOAT_STR1));
        assertTrue(Arrays.equals(qualifiers.get(11), FLOAT_STR2));
        assertTrue(Arrays.equals(qualifiers.get(12), INT1));
        assertTrue(Arrays.equals(qualifiers.get(13), INT2));
        assertTrue(Arrays.equals(qualifiers.get(14), INT_STR1));
        assertTrue(Arrays.equals(qualifiers.get(15), INT_STR2));
        assertTrue(Arrays.equals(qualifiers.get(16), LONG1));
        assertTrue(Arrays.equals(qualifiers.get(17), LONG2));
        assertTrue(Arrays.equals(qualifiers.get(18), LONG_STR1));
        assertTrue(Arrays.equals(qualifiers.get(19), LONG_STR2));
        assertTrue(Arrays.equals(qualifiers.get(20), SHORT1));
        assertTrue(Arrays.equals(qualifiers.get(21), SHORT2));
        assertTrue(Arrays.equals(qualifiers.get(22), SHORT_STR1));
        assertTrue(Arrays.equals(qualifiers.get(23), SHORT_STR2));
        assertTrue(Arrays.equals(qualifiers.get(24), STRING1));
        assertTrue(Arrays.equals(qualifiers.get(25), STRING2));
    }

    @Test
    public void getResultTest ()
    throws Exception {
        init();

        Result result = getRow(ROW);

        model = new Model(result);

        assertEquals(model, model.setResult(null));
        assertNull(model.getResult());
        assertEquals(result, model.setResult(result).getResult());
    }

    @Test
    public void getShortTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertEquals(new Short((short) 1), model.getShort(SHORT1));
        assertEquals(new Short((short) 2), model.getShort(SHORT2));

        thrown.expect(ModelException.class);
        model.getShort(NONEXISTENT);
    }

    @Test
    public void getShortDTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertEquals(new Short((short) 1), model.getShortD(SHORT1, (short) 1));
        assertEquals(new Short((short) 2), model.getShortD(NONEXISTENT, (short) 2));
        assertNull(model.getShortD(NONEXISTENT, null));
    }

    @Test
    public void getStringTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertEquals("simplebase", model.getString(STRING1));
        assertEquals("rocks", model.getString(STRING2));

        thrown.expect(ModelException.class);
        model.getString(NONEXISTENT);
    }

    @Test
    public void getStringDTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertEquals("simplebase", model.getStringD(STRING1, "simplebase"));
        assertEquals("rocks", model.getStringD(NONEXISTENT, "rocks"));
        assertNull(model.getStringD(NONEXISTENT, null));
    }

    @Test
    public void hasColumnTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertTrue(model.hasColumn(BOOLEAN1));
        assertTrue(model.hasColumn(BOOLEAN2));
        assertTrue(model.hasColumn(BOOLEAN_STR1));
        assertTrue(model.hasColumn(BOOLEAN_STR2));
        assertTrue(model.hasColumn(DOUBLE1));
        assertTrue(model.hasColumn(DOUBLE2));
        assertTrue(model.hasColumn(DOUBLE_STR1));
        assertTrue(model.hasColumn(DOUBLE_STR2));
        assertTrue(model.hasColumn(FLOAT1));
        assertTrue(model.hasColumn(FLOAT2));
        assertTrue(model.hasColumn(FLOAT_STR1));
        assertTrue(model.hasColumn(FLOAT_STR2));
        assertTrue(model.hasColumn(INT1));
        assertTrue(model.hasColumn(INT2));
        assertTrue(model.hasColumn(INT_STR1));
        assertTrue(model.hasColumn(INT_STR2));
        assertTrue(model.hasColumn(LONG1));
        assertTrue(model.hasColumn(LONG2));
        assertTrue(model.hasColumn(LONG_STR1));
        assertTrue(model.hasColumn(LONG_STR2));
        assertTrue(model.hasColumn(SHORT1));
        assertTrue(model.hasColumn(SHORT2));
        assertTrue(model.hasColumn(SHORT_STR1));
        assertTrue(model.hasColumn(SHORT_STR2));
        assertTrue(model.hasColumn(STRING1));
        assertTrue(model.hasColumn(STRING2));
        assertFalse(model.hasColumn(NONEXISTENT));
    }

    @Test
    public void hasFamilyTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertTrue(model.hasFamily(FAMILY1));
        assertFalse(model.hasFamily(NONEXISTENT));
    }

    @Test
    public void hasResultTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertTrue(model.hasResult());
    }

    @Test
    public void isEqualTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertTrue(model.isEqual(BOOLEAN1, BOOLEAN1));
        assertTrue(model.isEqual(DOUBLE1, DOUBLE1));
        assertTrue(model.isEqual(FLOAT1, FLOAT1));
        assertTrue(model.isEqual(INT1, INT1));
        assertTrue(model.isEqual(LONG1, LONG1));
        assertTrue(model.isEqual(SHORT1, SHORT1));
        assertTrue(model.isEqual(STRING1, STRING1));

        assertFalse(model.isEqual(BOOLEAN1, BOOLEAN2));
        assertFalse(model.isEqual(DOUBLE1, DOUBLE2));
        assertFalse(model.isEqual(FLOAT1, FLOAT2));
        assertFalse(model.isEqual(INT1, INT2));
        assertFalse(model.isEqual(LONG1, LONG2));
        assertFalse(model.isEqual(SHORT1, SHORT2));
        assertFalse(model.isEqual(STRING1, STRING2));
    }

    @Test
    public void parseBooleanTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertTrue(model.parseBoolean(BOOLEAN_STR1));
        assertFalse(model.parseBoolean(BOOLEAN_STR2));

        thrown.expect(ModelException.class);
        model.parseBoolean(NONEXISTENT);
    }

    @Test
    public void parseBooleanDTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertTrue(model.parseBooleanD(BOOLEAN_STR1, false));
        assertFalse(model.parseBooleanD(NONEXISTENT, false));

        assertNull(model.parseBooleanD(NONEXISTENT, null));
    }

    @Test
    public void parseDoubleTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertEquals(new Double(1.0), model.parseDouble(DOUBLE_STR1));
        assertEquals(new Double(2.0), model.parseDouble(DOUBLE_STR2));

        thrown.expect(ModelException.class);
        model.parseDouble(NONEXISTENT);
    }

    @Test
    public void parseDoubleDTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertEquals(new Double(1.0), model.parseDoubleD(DOUBLE_STR1, (double) 1.0));
        assertEquals(new Double(2.0), model.parseDoubleD(NONEXISTENT, (double) 2.0));

        assertNull(model.parseDoubleD(NONEXISTENT, null));
    }

    @Test
    public void parseIntTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertEquals(new Integer(1), model.parseInt(INT_STR1));
        assertEquals(new Integer(2), model.parseInt(INT_STR2));

        thrown.expect(ModelException.class);
        model.parseInt(NONEXISTENT);
    }

    @Test
    public void parseIntDTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertEquals(new Integer(1), model.parseIntD(INT_STR1, (int) 1));
        assertEquals(new Integer(2), model.parseIntD(NONEXISTENT, (int) 2));

        assertNull(model.parseIntD(NONEXISTENT, null));
    }

    @Test
    public void parseLongTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertEquals(new Long((long) 1), model.parseLong(LONG_STR1));
        assertEquals(new Long((long) 2), model.parseLong(LONG_STR2));

        thrown.expect(ModelException.class);
        model.parseLong(NONEXISTENT);
    }

    @Test
    public void parseLongDTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertEquals(new Long((long) 1), model.parseLongD(LONG_STR1, (long) 1));
        assertEquals(new Long((long) 2), model.parseLongD(NONEXISTENT, (long) 2));

        assertNull(model.parseLongD(NONEXISTENT, null));
    }

    @Test
    public void parseShortTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertEquals(new Short((short) 1), model.parseShort(SHORT_STR1));
        assertEquals(new Short((short) 2), model.parseShort(SHORT_STR2));

        thrown.expect(ModelException.class);
        model.parseShort(NONEXISTENT);
    }

    @Test
    public void parseShortDTest ()
    throws Exception {
        init();

        assertNotNull(model);
        assertEquals(new Short((short) 1), model.parseShortD(SHORT_STR1, (short) 1));
        assertEquals(new Short((short) 2), model.parseShortD(NONEXISTENT, (short) 2));

        assertNull(model.parseShortD(NONEXISTENT, null));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // HELPERS
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Initialize an individual test.
     */
    public void init ()
    throws Exception {
        switchTable(TABLE1);
        deleteRow(ROW);

        Put put = new Put(ROW);

        put.add(FAMILY1, BOOLEAN1,     Bytes.toBytes(true));
        put.add(FAMILY1, BOOLEAN2,     Bytes.toBytes(false));
        put.add(FAMILY1, DOUBLE1,      Bytes.toBytes((double) 1.0));
        put.add(FAMILY1, DOUBLE2,      Bytes.toBytes((double) 2.0));
        put.add(FAMILY1, FLOAT1,       Bytes.toBytes((float) 1.0));
        put.add(FAMILY1, FLOAT2,       Bytes.toBytes((float) 2.0));
        put.add(FAMILY1, INT1,         Bytes.toBytes((int) 1));
        put.add(FAMILY1, INT2,         Bytes.toBytes((int) 2));
        put.add(FAMILY1, LONG1,        Bytes.toBytes((long) 1));
        put.add(FAMILY1, LONG2,        Bytes.toBytes((long) 2));
        put.add(FAMILY1, SHORT1,       Bytes.toBytes((short) 1));
        put.add(FAMILY1, SHORT2,       Bytes.toBytes((short) 2));
        put.add(FAMILY1, STRING1,      Bytes.toBytes("simplebase"));
        put.add(FAMILY1, STRING2,      Bytes.toBytes("rocks"));
        put.add(FAMILY1, BOOLEAN_STR1, Bytes.toBytes("true"));
        put.add(FAMILY1, BOOLEAN_STR2, Bytes.toBytes("false"));
        put.add(FAMILY1, DOUBLE_STR1,  Bytes.toBytes("1.0"));
        put.add(FAMILY1, DOUBLE_STR2,  Bytes.toBytes("2.0"));
        put.add(FAMILY1, FLOAT_STR1,   Bytes.toBytes("1.0"));
        put.add(FAMILY1, FLOAT_STR2,   Bytes.toBytes("2.0"));
        put.add(FAMILY1, INT_STR1,     Bytes.toBytes("1"));
        put.add(FAMILY1, INT_STR2,     Bytes.toBytes("2"));
        put.add(FAMILY1, LONG_STR1,    Bytes.toBytes("1"));
        put.add(FAMILY1, LONG_STR2,    Bytes.toBytes("2"));
        put.add(FAMILY1, SHORT_STR1,   Bytes.toBytes("1"));
        put.add(FAMILY1, SHORT_STR2,   Bytes.toBytes("2"));

        writePut(put);
        flushTable();
        switchModel(ROW);
    }

    /**
     * Setup the test environment.
     */
    @BeforeClass
    public static void setup ()
    throws Exception {
        BaseTest.setup();
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