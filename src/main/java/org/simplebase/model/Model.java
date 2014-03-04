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

package org.simplebase.model;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * {@link Model} wraps an existing HBase <em>Result</em> instance and provides helper methods for retrieving and
 * comparing column values.
 *
 * <p>
 * <strong>Note:</strong> {@link Model} is reusable with multiple HBase <em>Result</em> instances.
 * </p>
 *
 * @author Sean Kerr [sean@code-box.org]
 */
public class Model {
    /** The nonexistent column error message. */
    private static final String ERR_NONEXISTENT_COLUMN = "Nonexistent column: '%s:%s'";

    /** The result. */
    private Result result;

    /**
     * Create a new Model instance.
     */
    public Model () {
    }

    /**
     * Create a new Model instance.
     *
     * @param result The result.
     */
    public Model (Result result) {
        assert result != null
             : "result == null";

        setResult(result);
    }

    /**
     * Find all qualifiers that match a prefix.
     *
     * @param family The column family.
     * @param prefix The prefix.
     */
    public List<byte[]> findQualifiers (byte[] family, byte[] prefix) {
        assert family != null && prefix != null
             : "family == null || prefix == null";

        List<byte[]> matches = new ArrayList();

        loop:
        for (Map.Entry<byte[],byte[]> entry : getResult().getFamilyMap(family).entrySet()) {
            byte[] qualifier = entry.getKey();

            if (prefix.length <= qualifier.length) {
                for (int i = 0; i < prefix.length; i++) {
                    if (prefix[i] != qualifier[i]) {
                        continue loop;
                    }
                }

                matches.add(qualifier);
            }
        }

        return matches;
    }

    /**
     * Find all qualifiers that match a prefix.
     *
     * @param family The column family.
     * @param prefix The prefix.
     */
    public List<byte[]> findQualifiers (byte[] family, String prefix) {
        return findQualifiers(family, Bytes.toBytes(prefix));
    }

    /**
     * Retrieve a boolean value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the value could not be retrieved.
     */
    public Boolean getBoolean (byte[] family, byte[] qualifier)
    throws ModelException {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        byte[] value = result.getValue(family, qualifier);

        if (value != null) {
            return Bytes.toBoolean(value);
        }

        throw new ModelException(ERR_NONEXISTENT_COLUMN, Bytes.toString(family), Bytes.toString(qualifier));
    }

    /**
     * Retrieve a boolean value.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Boolean getBoolean (byte[] family, byte[] qualifier, Boolean defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        byte[] value = result.getValue(family, qualifier);

        return value != null ? (Boolean) Bytes.toBoolean(value) : defaultValue;
    }

    /**
     * Retrieve a byte value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the value could not be retrieved.
     */
    public byte[] getBytes (byte[] family, byte[] qualifier)
    throws ModelException {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        byte[] value = result.getValue(family, qualifier);

        if (value != null) {
            return value;
        }

        throw new ModelException(ERR_NONEXISTENT_COLUMN, Bytes.toString(family), Bytes.toString(qualifier));
    }

    /**
     * Retrieve a byte value.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public byte[] getBytes (byte[] family, byte[] qualifier, byte[] defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        byte[] value = result.getValue(family, qualifier);

        return value != null ? value : defaultValue;
    }

    /**
     * Retrieve a double value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the value could not be retrieved.
     */
    public Double getDouble (byte[] family, byte[] qualifier)
    throws ModelException {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        byte[] value = result.getValue(family, qualifier);

        if (value != null) {
            return Bytes.toDouble(value);
        }

        throw new ModelException(ERR_NONEXISTENT_COLUMN, Bytes.toString(family), Bytes.toString(qualifier));
    }

    /**
     * Retrieve a double value.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Double getDouble (byte[] family, byte[] qualifier, Double defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        byte[] value = result.getValue(family, qualifier);

        return value != null ? (Double) Bytes.toDouble(value) : defaultValue;
    }

    /**
     * Retrieve a float value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the value could not be retrieved.
     */
    public Float getFloat (byte[] family, byte[] qualifier)
    throws ModelException {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        byte[] value = result.getValue(family, qualifier);

        if (value != null) {
            return Bytes.toFloat(value);
        }

        throw new ModelException(ERR_NONEXISTENT_COLUMN, Bytes.toString(family), Bytes.toString(qualifier));
    }

    /**
     * Retrieve a float value.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Float getFloat (byte[] family, byte[] qualifier, Float defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        byte[] value = result.getValue(family, qualifier);

        return value != null ? (Float) Bytes.toFloat(value) : defaultValue;
    }

    /**
     * Retrieve an integer value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the value could not be retrieved.
     */
    public Integer getInt (byte[] family, byte[] qualifier)
    throws ModelException {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        byte[] value = result.getValue(family, qualifier);

        if (value != null) {
            return Bytes.toInt(value);
        }

        throw new ModelException(ERR_NONEXISTENT_COLUMN, Bytes.toString(family), Bytes.toString(qualifier));
    }

    /**
     * Retrieve a integer value.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Integer getInt (byte[] family, byte[] qualifier, Integer defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        byte[] value = result.getValue(family, qualifier);

        return value != null ? (Integer) Bytes.toInt(value) : defaultValue;
    }

    /**
     * Retrieve a long value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the value could not be retrieved.
     */
    public Long getLong (byte[] family, byte[] qualifier)
    throws ModelException {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        byte[] value = result.getValue(family, qualifier);

        if (value != null) {
            return Bytes.toLong(value);
        }

        throw new ModelException(ERR_NONEXISTENT_COLUMN, Bytes.toString(family), Bytes.toString(qualifier));
    }

    /**
     * Retrieve a long value.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Long getLong (byte[] family, byte[] qualifier, Long defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        byte[] value = result.getValue(family, qualifier);

        return value != null ? (Long) Bytes.toLong(value) : defaultValue;
    }

    /**
     * Retrieve the underlying result.
     */
    public Result getResult () {
        return result;
    }

    /**
     * Retrieve a short value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the value could not be retrieved.
     */
    public Short getShort (byte[] family, byte[] qualifier)
    throws ModelException {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        byte[] value = result.getValue(family, qualifier);

        if (value != null) {
            return Bytes.toShort(value);
        }

        throw new ModelException(ERR_NONEXISTENT_COLUMN, Bytes.toString(family), Bytes.toString(qualifier));
    }

    /**
     * Retrieve a short value.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Short getShort (byte[] family, byte[] qualifier, Short defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        byte[] value = result.getValue(family, qualifier);

        return value != null ? (Short) Bytes.toShort(value) : defaultValue;
    }

    /**
     * Retrieve a string value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the value could not be retrieved.
     */
    public String getString (byte[] family, byte[] qualifier)
    throws ModelException {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        byte[] value = result.getValue(family, qualifier);

        if (value != null) {
            return Bytes.toString(value);
        }

        throw new ModelException(ERR_NONEXISTENT_COLUMN, Bytes.toString(family), Bytes.toString(qualifier));
    }

    /**
     * Retrieve a string value.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public String getString (byte[] family, byte[] qualifier, String defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        byte[] value = result.getValue(family, qualifier);

        return value != null ? Bytes.toString(value) : defaultValue;
    }

    /**
     * Indicates that a column is present.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     */
    public boolean hasColumn (byte[] family, byte[] qualifier) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        return result.containsColumn(family, qualifier);
    }

    /**
     * Indicates that all columns are present.
     *
     * @param family     The column family.
     * @param qualifiers The qualifiers.
     */
    public boolean hasColumns (byte[] family, byte[]... qualifiers) {
        assert family != null && qualifiers != null
             : "family == null || qualifiers == null";

        for (byte[] qualifier : qualifiers) {
            if (!result.containsColumn(family, qualifier)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Indicates that the underlying result is present.
     */
    public boolean hasResult () {
        return result != null;
    }

    /**
     * Compare two double columns to see if the first column is greater than the second column.
     *
     * @param family     The column family.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isDoubleGreater (byte[] family, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return isDoubleGreater(family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two double columns to see if the first column is greater than the second column.
     *
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isDoubleGreater (byte[] family1, byte[] qualifier1, byte[] family2, byte[] qualifier2)
    throws ModelException {
        assert family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        return getDouble(family1, qualifier1) > getDouble(family2, qualifier2);
    }

    /**
     * Compare two double columns to see if the first column is less than the second column.
     *
     * @param family     The column family.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isDoubleLesser (byte[] family, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return isDoubleLesser(family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two double columns to see if the first column is less than the second column.
     *
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isDoubleLesser (byte[] family1, byte[] qualifier1, byte[] family2, byte[] qualifier2)
    throws ModelException {
        assert family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        return getDouble(family1, qualifier1) < getDouble(family2, qualifier2);
    }

    /**
     * Compare two double string columns to see if the first column is greater than the second column.
     *
     * @param family     The column family.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isDoubleStrGreater (byte[] family, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return isDoubleStrGreater(family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two double string columns to see if the first column is greater than the second column.
     *
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isDoubleStrGreater (byte[] family1, byte[] qualifier1, byte[] family2, byte[] qualifier2)
    throws ModelException {
        assert family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        return parseDouble(family1, qualifier1) > parseDouble(family2, qualifier2);
    }

    /**
     * Compare two double string columns to see if the first column is less than the second column.
     *
     * @param family     The column family.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isDoubleStrLesser (byte[] family, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return isDoubleStrLesser(family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two double string columns to see if the first column is less than the second column.
     *
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isDoubleStrLesser (byte[] family1, byte[] qualifier1, byte[] family2, byte[] qualifier2)
    throws ModelException {
        assert family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        return parseDouble(family1, qualifier1) < parseDouble(family2, qualifier2);
    }

    /**
     * Compare two columns for equality.
     *
     * @param family     The column family.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isEqual (byte[] family, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return Arrays.equals(getBytes(family, qualifier1), getBytes(family, qualifier2));
    }

    /**
     * Compare two columns for equality.
     *
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isEqual (byte[] family1, byte[] qualifier1, byte[] family2, byte[] qualifier2)
    throws ModelException {
        assert family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        return Arrays.equals(getBytes(family1, qualifier1), getBytes(family2, qualifier2));
    }

    /**
     * Compare two float columns to see if the first column is greater than the second column.
     *
     * @param family     The column family.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isFloatGreater (byte[] family, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return isFloatGreater(family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two float columns to see if the first column is greater than the second column.
     *
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isFloatGreater (byte[] family1, byte[] qualifier1, byte[] family2, byte[] qualifier2)
    throws ModelException {
        assert family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        return getFloat(family1, qualifier1) > getFloat(family2, qualifier2);
    }

    /**
     * Compare two float columns to see if the first column is less than the second column.
     *
     * @param family     The column family.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isFloatLesser (byte[] family, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return isFloatLesser(family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two float columns to see if the first column is less than the second column.
     *
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isFloatLesser (byte[] family1, byte[] qualifier1, byte[] family2, byte[] qualifier2)
    throws ModelException {
        assert family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        return getFloat(family1, qualifier1) < getFloat(family2, qualifier2);
    }

    /**
     * Compare two float string columns to see if the first column is greater than the second column.
     *
     * @param family     The column family.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isFloatStrGreater (byte[] family, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return isFloatStrGreater(family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two float string columns to see if the first column is greater than the second column.
     *
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isFloatStrGreater (byte[] family1, byte[] qualifier1, byte[] family2, byte[] qualifier2)
    throws ModelException {
        assert family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        return parseFloat(family1, qualifier1) > parseFloat(family2, qualifier2);
    }

    /**
     * Compare two float string columns to see if the first column is less than the second column.
     *
     * @param family     The column family.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isFloatStrLesser (byte[] family, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return isFloatStrLesser(family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two float string columns to see if the first column is less than the second column.
     *
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isFloatStrLesser (byte[] family1, byte[] qualifier1, byte[] family2, byte[] qualifier2)
    throws ModelException {
        assert family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        return parseFloat(family1, qualifier1) < parseFloat(family2, qualifier2);
    }

    /**
     * Compare two integer columns to see if the first column is greater than the second column.
     *
     * @param family     The column family.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isIntGreater (byte[] family, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return isIntGreater(family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two integer columns to see if the first column is greater than the second column.
     *
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isIntGreater (byte[] family1, byte[] qualifier1, byte[] family2, byte[] qualifier2)
    throws ModelException {
        assert family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        return getInt(family1, qualifier1) > getInt(family2, qualifier2);
    }

    /**
     * Compare two integer columns to see if the first column is less than the second column.
     *
     * @param family     The column family.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isIntLesser (byte[] family, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return isIntLesser(family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two integer columns to see if the first column is less than the second column.
     *
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isIntLesser (byte[] family1, byte[] qualifier1, byte[] family2, byte[] qualifier2)
    throws ModelException {
        assert family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        return getInt(family1, qualifier1) < getInt(family2, qualifier2);
    }

    /**
     * Compare two integer string columns to see if the first column is greater than the second column.
     *
     * @param family     The column family.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isIntStrGreater (byte[] family, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return isIntStrGreater(family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two integer string columns to see if the first column is greater than the second column.
     *
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isIntStrGreater (byte[] family1, byte[] qualifier1, byte[] family2, byte[] qualifier2)
    throws ModelException {
        assert family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        return parseInt(family1, qualifier1) > parseInt(family2, qualifier2);
    }

    /**
     * Compare two integer string columns to see if the first column is less than the second column.
     *
     * @param family     The column family.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isIntStrLesser (byte[] family, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return isIntStrLesser(family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two integer string columns to see if the first column is less than the second column.
     *
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isIntStrLesser (byte[] family1, byte[] qualifier1, byte[] family2, byte[] qualifier2)
    throws ModelException {
        assert family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        return parseInt(family1, qualifier1) < parseInt(family2, qualifier2);
    }

    /**
     * Compare two long columns to see if the first column is greater than the second column.
     *
     * @param family     The column family.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isLongGreater (byte[] family, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return isLongGreater(family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two long columns to see if the first column is greater than the second column.
     *
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isLongGreater (byte[] family1, byte[] qualifier1, byte[] family2, byte[] qualifier2)
    throws ModelException {
        assert family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        return getLong(family1, qualifier1) > getLong(family2, qualifier2);
    }

    /**
     * Compare two long columns to see if the first column is less than the second column.
     *
     * @param family     The column family.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isLongLesser (byte[] family, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return isLongLesser(family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two long columns to see if the first column is less than the second column.
     *
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isLongLesser (byte[] family1, byte[] qualifier1, byte[] family2, byte[] qualifier2)
    throws ModelException {
        assert family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        return getLong(family1, qualifier1) < getLong(family2, qualifier2);
    }

    /**
     * Compare two long string columns to see if the first column is greater than the second column.
     *
     * @param family     The column family.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isLongStrGreater (byte[] family, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return isLongStrGreater(family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two long string columns to see if the first column is greater than the second column.
     *
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isLongStrGreater (byte[] family1, byte[] qualifier1, byte[] family2, byte[] qualifier2)
    throws ModelException {
        assert family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        return parseLong(family1, qualifier1) > parseLong(family2, qualifier2);
    }

    /**
     * Compare two long string columns to see if the first column is less than the second column.
     *
     * @param family     The column family.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isLongStrLesser (byte[] family, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return isLongStrLesser(family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two long string columns to see if the first column is less than the second column.
     *
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isLongStrLesser (byte[] family1, byte[] qualifier1, byte[] family2, byte[] qualifier2)
    throws ModelException {
        assert family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        return parseLong(family1, qualifier1) < parseLong(family2, qualifier2);
    }

    /**
     * Compare two short columns to see if the first column is greater than the second column.
     *
     * @param family     The column family.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isShortGreater (byte[] family, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return isShortGreater(family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two short columns to see if the first column is greater than the second column.
     *
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isShortGreater (byte[] family1, byte[] qualifier1, byte[] family2, byte[] qualifier2)
    throws ModelException {
        assert family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        return getShort(family1, qualifier1) > getShort(family2, qualifier2);
    }

    /**
     * Compare two short columns to see if the first column is less than the second column.
     *
     * @param family     The column family.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isShortLesser (byte[] family, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return isShortLesser(family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two short columns to see if the first column is less than the second column.
     *
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isShortLesser (byte[] family1, byte[] qualifier1, byte[] family2, byte[] qualifier2)
    throws ModelException {
        assert family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        return getShort(family1, qualifier1) < getShort(family2, qualifier2);
    }

    /**
     * Compare two short string columns to see if the first column is greater than the second column.
     *
     * @param family     The column family.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isShortStrGreater (byte[] family, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return isShortStrGreater(family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two short string columns to see if the first column is greater than the second column.
     *
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isShortStrGreater (byte[] family1, byte[] qualifier1, byte[] family2, byte[] qualifier2)
    throws ModelException {
        assert family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        return parseShort(family1, qualifier1) > parseShort(family2, qualifier2);
    }

    /**
     * Compare two short string columns to see if the first column is less than the second column.
     *
     * @param family     The column family.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isShortStrLesser (byte[] family, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return isShortStrLesser(family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two short string columns to see if the first column is less than the second column.
     *
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isShortStrLesser (byte[] family1, byte[] qualifier1, byte[] family2, byte[] qualifier2)
    throws ModelException {
        assert family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        return parseShort(family1, qualifier1) < parseShort(family2, qualifier2);
    }

    /**
     * Parse a boolean value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the value could not be retrieved.
     */
    public Boolean parseBoolean (byte[] family, byte[] qualifier)
    throws ModelException {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        String value = getString(family, qualifier, null);

        if (value != null) {
            return Boolean.parseBoolean(value);
        }

        throw new ModelException(ERR_NONEXISTENT_COLUMN, Bytes.toString(family), Bytes.toString(qualifier));
    }

    /**
     * Parse a boolean value.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Boolean parseBoolean (byte[] family, byte[] qualifier, Boolean defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        String value = getString(family, qualifier, null);

        return value != null ? (Boolean) Boolean.parseBoolean(value) : defaultValue;
    }

    /**
     * Parse a double value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the value could not be retrieved.
     */
    public Double parseDouble (byte[] family, byte[] qualifier)
    throws ModelException {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        String value = getString(family, qualifier, null);

        if (value != null) {
            return Double.parseDouble(value);
        }

        throw new ModelException(ERR_NONEXISTENT_COLUMN, Bytes.toString(family), Bytes.toString(qualifier));
    }

    /**
     * Parse a double value.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Double parseDouble (byte[] family, byte[] qualifier, Double defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        String value = getString(family, qualifier, null);

        return value != null ? (Double) Double.parseDouble(value) : defaultValue;
    }

    /**
     * Parse a float value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the value could not be retrieved.
     */
    public Float parseFloat (byte[] family, byte[] qualifier)
    throws ModelException {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        String value = getString(family, qualifier, null);

        if (value != null) {
            return Float.parseFloat(value);
        }

        throw new ModelException(ERR_NONEXISTENT_COLUMN, Bytes.toString(family), Bytes.toString(qualifier));
    }

    /**
     * Parse a float value.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Float parseFloat (byte[] family, byte[] qualifier, Float defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        String value = getString(family, qualifier, null);

        return value != null ? (Float) Float.parseFloat(value) : defaultValue;
    }

    /**
     * Parse an integer value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the value could not be retrieved.
     */
    public Integer parseInt (byte[] family, byte[] qualifier)
    throws ModelException {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        String value = getString(family, qualifier, null);

        if (value != null) {
            return Integer.parseInt(value);
        }

        throw new ModelException(ERR_NONEXISTENT_COLUMN, Bytes.toString(family), Bytes.toString(qualifier));
    }

    /**
     * Parse an integer value.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Integer parseInt (byte[] family, byte[] qualifier, Integer defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        String value = getString(family, qualifier, null);

        return value != null ? (Integer) Integer.parseInt(value) : defaultValue;
    }

    /**
     * Parse a long value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the value could not be retrieved.
     */
    public Long parseLong (byte[] family, byte[] qualifier)
    throws ModelException {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        String value = getString(family, qualifier, null);

        if (value != null) {
            return Long.parseLong(value);
        }

        throw new ModelException(ERR_NONEXISTENT_COLUMN, Bytes.toString(family), Bytes.toString(qualifier));
    }

    /**
     * Parse a long value.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Long parseLong (byte[] family, byte[] qualifier, Long defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        String value = getString(family, qualifier, null);

        return value != null ? (Long) Long.parseLong(value) : defaultValue;
    }

    /**
     * Parse a short value.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the value could not be retrieved.
     */
    public Short parseShort (byte[] family, byte[] qualifier)
    throws ModelException {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        String value = getString(family, qualifier, null);

        if (value != null) {
            return Short.parseShort(value);
        }

        throw new ModelException(ERR_NONEXISTENT_COLUMN, Bytes.toString(family), Bytes.toString(qualifier));
    }

    /**
     * Parse a short value.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Short parseShort (byte[] family, byte[] qualifier, Short defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        String value = getString(family, qualifier, null);

        return value != null ? (Short) Short.parseShort(value) : defaultValue;
    }

    /**
     * Set the underlying result.
     *
     * @param result The result.
     */
    public Model setResult (Result result) {
        this.result = result;

        return this;
    }
}