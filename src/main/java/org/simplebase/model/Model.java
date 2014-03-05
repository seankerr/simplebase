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
    /** The equality comparison. */
    public static final CompareOp COMPARE_EQ = CompareOp.EQ;

    /** The greater than comparison. */
    public static final CompareOp COMPARE_GT = CompareOp.GT;

    /** The less than comparison. */
    public static final CompareOp COMPARE_LT = CompareOp.LT;

    /** The nonexistent column error message. */
    private static final String ERR_NONEXISTENT_COLUMN = "Nonexistent column: '%s:%s'";

    /** The default column family. */
    private byte[] family;

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
     * Compare two binary double columns, or throw an exception if either is null.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param operation  The comparison operation.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean compareDouble (CompareOp operation, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return compareDouble(operation, family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two binary double columns, or throw an exception if either is null.
     *
     * @param operation  The comparison operation.
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean compareDouble (CompareOp operation, byte[] family1, byte[] qualifier1, byte[] family2,
                                  byte[] qualifier2)
    throws ModelException {
        assert operation != null && family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "operation == null || family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        switch (operation) {
            case EQ:
                return (double) getDouble(family1, qualifier1) == (double) getDouble(family2, qualifier2);
            case LT:
                return (double) getDouble(family1, qualifier1) < (double) getDouble(family2, qualifier2);
            default:
                return (double) getDouble(family1, qualifier1) > (double) getDouble(family2, qualifier2);
        }
    }

    /**
     * Compare two string double columns, or throw an exception if either is null.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param operation  The comparison operation.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean compareDoubleS (CompareOp operation, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return compareDoubleS(operation, family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two string double columns, or throw an exception if either is null.
     *
     * @param operation  The comparison operation.
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean compareDoubleS (CompareOp operation, byte[] family1, byte[] qualifier1, byte[] family2,
                                   byte[] qualifier2)
    throws ModelException {
        assert operation != null && family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "operation == null || family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        switch (operation) {
            case EQ:
                return (double) parseDouble(family1, qualifier1) == (double) parseDouble(family2, qualifier2);
            case LT:
                return (double) parseDouble(family1, qualifier1) < (double) parseDouble(family2, qualifier2);
            default:
                return (double) parseDouble(family1, qualifier1) > (double) parseDouble(family2, qualifier2);
        }
    }

    /**
     * Compare two binary float columns, or throw an exception if either is null.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param operation  The comparison operation.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean compareFloat (CompareOp operation, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return compareFloat(operation, family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two binary float columns, or throw an exception if either is null.
     *
     * @param operation  The comparison operation.
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean compareFloat (CompareOp operation, byte[] family1, byte[] qualifier1, byte[] family2,
                                 byte[] qualifier2)
    throws ModelException {
        assert operation != null && family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "operation == null || family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        switch (operation) {
            case EQ:
                return (float) getFloat(family1, qualifier1) == (float) getFloat(family2, qualifier2);
            case LT:
                return (float) getFloat(family1, qualifier1) < (float) getFloat(family2, qualifier2);
            default:
                return (float) getFloat(family1, qualifier1) > (float) getFloat(family2, qualifier2);
        }
    }

    /**
     * Compare two string float columns, or throw an exception if either is null.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param operation  The comparison operation.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean compareFloatS (CompareOp operation, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return compareFloatS(operation, family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two string float columns, or throw an exception if either is null.
     *
     * @param operation  The comparison operation.
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean compareFloatS (CompareOp operation, byte[] family1, byte[] qualifier1, byte[] family2,
                                  byte[] qualifier2)
    throws ModelException {
        assert operation != null && family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "operation == null || family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        switch (operation) {
            case EQ:
                return (float) parseFloat(family1, qualifier1) == (float) parseFloat(family2, qualifier2);
            case LT:
                return (float) parseFloat(family1, qualifier1) < (float) parseFloat(family2, qualifier2);
            default:
                return (float) parseFloat(family1, qualifier1) > (float) parseFloat(family2, qualifier2);
        }
    }

    /**
     * Compare two binary int columns, or throw an exception if either is null.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param operation  The comparison operation.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean compareInt (CompareOp operation, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return compareInt(operation, family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two binary int columns, or throw an exception if either is null.
     *
     * @param operation  The comparison operation.
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean compareInt (CompareOp operation, byte[] family1, byte[] qualifier1, byte[] family2,
                               byte[] qualifier2)
    throws ModelException {
        assert operation != null && family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "operation == null || family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        switch (operation) {
            case EQ:
                return getInt(family1, qualifier1) == getInt(family2, qualifier2);
            case LT:
                return getInt(family1, qualifier1) < getInt(family2, qualifier2);
            default:
                return getInt(family1, qualifier1) > getInt(family2, qualifier2);
        }
    }

    /**
     * Compare two string int columns, or throw an exception if either is null.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param operation  The comparison operation.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean compareIntS (CompareOp operation, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return compareIntS(operation, family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two string int columns, or throw an exception if either is null.
     *
     * @param operation  The comparison operation.
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean compareIntS (CompareOp operation, byte[] family1, byte[] qualifier1, byte[] family2,
                                byte[] qualifier2)
    throws ModelException {
        assert operation != null && family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "operation == null || family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        switch (operation) {
            case EQ:
                return parseInt(family1, qualifier1) == parseInt(family2, qualifier2);
            case LT:
                return parseInt(family1, qualifier1) < parseInt(family2, qualifier2);
            default:
                return parseInt(family1, qualifier1) > parseInt(family2, qualifier2);
        }
    }

    /**
     * Compare two binary long columns, or throw an exception if either is null.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param operation  The comparison operation.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean compareLong (CompareOp operation, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return compareLong(operation, family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two binary long columns, or throw an exception if either is null.
     *
     * @param operation  The comparison operation.
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean compareLong (CompareOp operation, byte[] family1, byte[] qualifier1, byte[] family2,
                                byte[] qualifier2)
    throws ModelException {
        assert operation != null && family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "operation == null || family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        switch (operation) {
            case EQ:
                return getLong(family1, qualifier1) == getLong(family2, qualifier2);
            case LT:
                return getLong(family1, qualifier1) < getLong(family2, qualifier2);
            default:
                return getLong(family1, qualifier1) > getLong(family2, qualifier2);
        }
    }

    /**
     * Compare two string long columns, or throw an exception if either is null.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param operation  The comparison operation.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean compareLongS (CompareOp operation, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return compareLongS(operation, family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two string long columns, or throw an exception if either is null.
     *
     * @param operation  The comparison operation.
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean compareLongS (CompareOp operation, byte[] family1, byte[] qualifier1, byte[] family2,
                                 byte[] qualifier2)
    throws ModelException {
        assert operation != null && family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "operation == null || family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        switch (operation) {
            case EQ:
                return parseLong(family1, qualifier1) == parseLong(family2, qualifier2);
            case LT:
                return parseLong(family1, qualifier1) < parseLong(family2, qualifier2);
            default:
                return parseLong(family1, qualifier1) > parseLong(family2, qualifier2);
        }
    }

    /**
     * Compare two binary short columns, or throw an exception if either is null.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param operation  The comparison operation.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean compareShort (CompareOp operation, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return compareShort(operation, family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two binary short columns, or throw an exception if either is null.
     *
     * @param operation  The comparison operation.
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean compareShort (CompareOp operation, byte[] family1, byte[] qualifier1, byte[] family2,
                                byte[] qualifier2)
    throws ModelException {
        assert operation != null && family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "operation == null || family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        switch (operation) {
            case EQ:
                return getShort(family1, qualifier1) == getShort(family2, qualifier2);
            case LT:
                return getShort(family1, qualifier1) < getShort(family2, qualifier2);
            default:
                return getShort(family1, qualifier1) > getShort(family2, qualifier2);
        }
    }

    /**
     * Compare two string short columns, or throw an exception if either is null.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param operation  The comparison operation.
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean compareShortS (CompareOp operation, byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return compareShortS(operation, family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two string short columns, or throw an exception if either is null.
     *
     * @param operation  The comparison operation.
     * @param family1    The first column family.
     * @param qualifier1 The first qualifier.
     * @param family2    The second column family.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean compareShortS (CompareOp operation, byte[] family1, byte[] qualifier1, byte[] family2,
                                 byte[] qualifier2)
    throws ModelException {
        assert operation != null && family1 != null && qualifier1 != null && family2 != null && qualifier2 != null
             : "operation == null || family1 == null || qualifier1 == null || family2 == null || qualifier2 == null";

        switch (operation) {
            case EQ:
                return parseShort(family1, qualifier1) == parseShort(family2, qualifier2);
            case LT:
                return parseShort(family1, qualifier1) < parseShort(family2, qualifier2);
            default:
                return parseShort(family1, qualifier1) > parseShort(family2, qualifier2);
        }
    }

    /**
     * Find all qualifiers that match a prefix.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param prefix The prefix.
     */
    public List<byte[]> findQualifiers (byte[] prefix) {
        return findQualifiers(family, prefix);
    }

    /**
     * Find all qualifiers that match a prefix.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param prefix The prefix.
     */
    public List<byte[]> findQualifiers (String prefix) {
        return findQualifiers(family, Bytes.toBytes(prefix));
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
     * Retrieve a binary boolean value, or throw an exception if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
     */
    public Boolean getBoolean (byte[] qualifier)
    throws ModelException {
        return getBoolean(family, qualifier);
    }

    /**
     * Retrieve a binary boolean value, or throw an exception if the column is nonexistent.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
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
     * Retrieve a binary boolean value, or return the default value if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Boolean getBooleanD (byte[] qualifier, Boolean defaultValue) {
        return getBooleanD(family, qualifier, defaultValue);
    }

    /**
     * Retrieve a binary boolean value, or return the default value if the column is nonexistent.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Boolean getBooleanD (byte[] family, byte[] qualifier, Boolean defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        byte[] value = result.getValue(family, qualifier);

        // the cast avoids an ugly auto-boxing bug when defaultValue is null
        return value != null ? (Boolean) Bytes.toBoolean(value) : defaultValue;
    }

    /**
     * Retrieve a binary byte value, or throw an exception if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
     */
    public byte[] getBytes (byte[] qualifier)
    throws ModelException {
        return getBytes(family, qualifier);
    }

    /**
     * Retrieve a binary byte value, or throw an exception if the column is nonexistent.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
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
     * Retrieve a binary byte value, or return the default value if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public byte[] getBytesD (byte[] qualifier, byte[] defaultValue) {
        return getBytesD(family, qualifier, defaultValue);
    }

    /**
     * Retrieve a binary byte value, or return the default value if the column is nonexistent.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public byte[] getBytesD (byte[] family, byte[] qualifier, byte[] defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        byte[] value = result.getValue(family, qualifier);

        return value != null ? value : defaultValue;
    }

    /**
     * Retrieve the default column family.
     */
    public byte[] getColumnFamily () {
        return family;
    }

    /**
     * Retrieve a binary double value, or throw an exception if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
     */
    public Double getDouble (byte[] qualifier)
    throws ModelException {
        return getDouble(family, qualifier);
    }

    /**
     * Retrieve a binary double value, or throw an exception if the column is nonexistent.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
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
     * Retrieve a binary double value, or return the default value if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Double getDoubleD (byte[] qualifier, Double defaultValue) {
        return getDoubleD(family, qualifier, defaultValue);
    }

    /**
     * Retrieve a binary double value, or return the default value if the column is nonexistent.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Double getDoubleD (byte[] family, byte[] qualifier, Double defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        byte[] value = result.getValue(family, qualifier);

        // the cast avoids an ugly auto-boxing bug when defaultValue is null
        return value != null ? (Double) Bytes.toDouble(value) : defaultValue;
    }

    /**
     * Retrieve a list of column families.
     */
    public List<byte[]> getFamilies () {
        assert result != null
             : "result == null";

        return new ArrayList(result.getNoVersionMap().keySet());
    }

    /**
     * Retrieve a binary float value, or throw an exception if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
     */
    public Float getFloat (byte[] qualifier)
    throws ModelException {
        return getFloat(family, qualifier);
    }

    /**
     * Retrieve a binary float value, or throw an exception if the column is nonexistent.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
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
     * Retrieve a binary float value, or return the default value if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Float getFloatD (byte[] qualifier, Float defaultValue) {
        return getFloatD(family, qualifier, defaultValue);
    }

    /**
     * Retrieve a binary float value, or return the default value if the column is nonexistent.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Float getFloatD (byte[] family, byte[] qualifier, Float defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        byte[] value = result.getValue(family, qualifier);

        // the cast avoids an ugly auto-boxing bug when defaultValue is null
        return value != null ? (Float) Bytes.toFloat(value) : defaultValue;
    }

    /**
     * Retrieve an binary int value, or throw an exception if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
     */
    public Integer getInt (byte[] qualifier)
    throws ModelException {
        return getInt(family, qualifier);
    }

    /**
     * Retrieve an binary int value, or throw an exception if the column is nonexistent.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
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
     * Retrieve a binary int value, or return the default value if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Integer getIntD (byte[] qualifier, Integer defaultValue) {
        return getIntD(family, qualifier, defaultValue);
    }

    /**
     * Retrieve a binary int value, or return the default value if the column is nonexistent.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Integer getIntD (byte[] family, byte[] qualifier, Integer defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        byte[] value = result.getValue(family, qualifier);

        // the cast avoids an ugly auto-boxing bug when defaultValue is null
        return value != null ? (Integer) Bytes.toInt(value) : defaultValue;
    }

    /**
     * Retrieve a binary long value, or throw an exception if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
     */
    public Long getLong (byte[] qualifier)
    throws ModelException {
        return getLong(family, qualifier);
    }

    /**
     * Retrieve a binary long value, or throw an exception if the column is nonexistent.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
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
     * Retrieve a binary long value, or return the default value if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Long getLongD (byte[] qualifier, Long defaultValue) {
        return getLongD(family, qualifier, defaultValue);
    }

    /**
     * Retrieve a binary long value, or return the default value if the column is nonexistent.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Long getLongD (byte[] family, byte[] qualifier, Long defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        byte[] value = result.getValue(family, qualifier);

        // the cast avoids an ugly auto-boxing bug when defaultValue is null
        return value != null ? (Long) Bytes.toLong(value) : defaultValue;
    }

    /**
     * Retrieve a list of qualifiers.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     */
    public List<byte[]> getQualifiers () {
        return getQualifiers(family);
    }

    /**
     * Retrieve a list of qualifiers.
     *
     * @param family The column family.
     */
    public List<byte[]> getQualifiers (byte[] family) {
        assert result != null
             : "result == null";

        return new ArrayList(result.getFamilyMap(family).keySet());
    }

    /**
     * Retrieve the underlying result.
     */
    public Result getResult () {
        return result;
    }

    /**
     * Retrieve a binary short value, or throw an exception if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
     */
    public Short getShort (byte[] qualifier)
    throws ModelException {
        return getShort(family, qualifier);
    }

    /**
     * Retrieve a binary short value, or throw an exception if the column is nonexistent.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
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
     * Retrieve a binary short value, or return the default value if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Short getShortD (byte[] qualifier, Short defaultValue) {
        return getShortD(family, qualifier, defaultValue);
    }

    /**
     * Retrieve a binary short value, or return the default value if the column is nonexistent.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Short getShortD (byte[] family, byte[] qualifier, Short defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        byte[] value = result.getValue(family, qualifier);

        // the cast avoids an ugly auto-boxing bug when defaultValue is null
        return value != null ? (Short) Bytes.toShort(value) : defaultValue;
    }

    /**
     * Retrieve a string value, or throw an exception if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
     */
    public String getString (byte[] qualifier)
    throws ModelException {
        return getString(family, qualifier);
    }

    /**
     * Retrieve a string value, or throw an exception if the column is nonexistent.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
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
     * Retrieve a string value, or return the default value if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public String getStringD (byte[] qualifier, String defaultValue) {
        return getStringD(family, qualifier, defaultValue);
    }

    /**
     * Retrieve a string value, or return the default value if the column is nonexistent.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public String getStringD (byte[] family, byte[] qualifier, String defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        byte[] value = result.getValue(family, qualifier);

        return value != null ? Bytes.toString(value) : defaultValue;
    }

    /**
     * Indicates that a column is present.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier The qualifier.
     */
    public boolean hasColumn (byte[] qualifier) {
        return hasColumn(family, qualifier);
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
     * Indicates that a column family is present.
     *
     * @param family The column family.
     */
    public boolean hasFamily (byte[] family) {
        assert result != null
             : "result == null";

        return result.getNoVersionMap().containsKey(family);
    }

    /**
     * Indicates that the underlying result is present.
     */
    public boolean hasResult () {
        return result != null;
    }

    /**
     * Compare two columns byte-for-byte.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier1 The first qualifier.
     * @param qualifier2 The second qualifier.
     *
     * @throws ModelException If either column is nonexistent.
     */
    public boolean isEqual (byte[] qualifier1, byte[] qualifier2)
    throws ModelException {
        return isEqual(family, qualifier1, family, qualifier2);
    }

    /**
     * Compare two columns byte-for-byte.
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
     * Parse a string boolean value, or throw an exception if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
     */
    public Boolean parseBoolean (byte[] qualifier)
    throws ModelException {
        return parseBoolean(family, qualifier);
    }

    /**
     * Parse a string boolean value, or throw an exception if the column is nonexistent.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
     */
    public Boolean parseBoolean (byte[] family, byte[] qualifier)
    throws ModelException {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        String value = getStringD(family, qualifier, null);

        if (value != null) {
            return Boolean.parseBoolean(value);
        }

        throw new ModelException(ERR_NONEXISTENT_COLUMN, Bytes.toString(family), Bytes.toString(qualifier));
    }

    /**
     * Parse a string boolean value, or return the default value if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Boolean parseBooleanD (byte[] qualifier, Boolean defaultValue) {
        return parseBooleanD(family, qualifier, defaultValue);
    }

    /**
     * Parse a string boolean value, or return the default value if the column is nonexistent.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Boolean parseBooleanD (byte[] family, byte[] qualifier, Boolean defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        String value = getStringD(family, qualifier, null);

        // the cast avoids an ugly auto-boxing bug when defaultValue is null
        return value != null ? (Boolean) Boolean.parseBoolean(value) : defaultValue;
    }

    /**
     * Parse a string double value, or throw an exception if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
     */
    public Double parseDouble (byte[] qualifier)
    throws ModelException {
        return parseDouble(family, qualifier);
    }

    /**
     * Parse a string double value, or throw an exception if the column is nonexistent.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
     */
    public Double parseDouble (byte[] family, byte[] qualifier)
    throws ModelException {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        String value = getStringD(family, qualifier, null);

        if (value != null) {
            return Double.parseDouble(value);
        }

        throw new ModelException(ERR_NONEXISTENT_COLUMN, Bytes.toString(family), Bytes.toString(qualifier));
    }

    /**
     * Parse a string double value, or return the default value if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Double parseDoubleD (byte[] qualifier, Double defaultValue) {
        return parseDoubleD(family, qualifier, defaultValue);
    }

    /**
     * Parse a string double value, or return the default value if the column is nonexistent.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Double parseDoubleD (byte[] family, byte[] qualifier, Double defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        String value = getStringD(family, qualifier, null);

        // the cast avoids an ugly auto-boxing bug when defaultValue is null
        return value != null ? (Double) Double.parseDouble(value) : defaultValue;
    }

    /**
     * Parse a string float value, or throw an exception if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
     */
    public Float parseFloat (byte[] qualifier)
    throws ModelException {
        return parseFloat(family, qualifier);
    }

    /**
     * Parse a string float value, or throw an exception if the column is nonexistent.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
     */
    public Float parseFloat (byte[] family, byte[] qualifier)
    throws ModelException {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        String value = getStringD(family, qualifier, null);

        if (value != null) {
            return Float.parseFloat(value);
        }

        throw new ModelException(ERR_NONEXISTENT_COLUMN, Bytes.toString(family), Bytes.toString(qualifier));
    }

    /**
     * Parse a string float value, or return the default value if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Float parseFloatD (byte[] qualifier, Float defaultValue) {
        return parseFloatD(family, qualifier, defaultValue);
    }

    /**
     * Parse a string float value, or return the default value if the column is nonexistent.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Float parseFloatD (byte[] family, byte[] qualifier, Float defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        String value = getStringD(family, qualifier, null);

        // the cast avoids an ugly auto-boxing bug when defaultValue is null
        return value != null ? (Float) Float.parseFloat(value) : defaultValue;
    }

    /**
     * Parse a stringn integer value, or throw an exception if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
     */
    public Integer parseInt (byte[] qualifier)
    throws ModelException {
        return parseInt(family, qualifier);
    }

    /**
     * Parse a stringn integer value, or throw an exception if the column is nonexistent.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
     */
    public Integer parseInt (byte[] family, byte[] qualifier)
    throws ModelException {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        String value = getStringD(family, qualifier, null);

        if (value != null) {
            return Integer.parseInt(value);
        }

        throw new ModelException(ERR_NONEXISTENT_COLUMN, Bytes.toString(family), Bytes.toString(qualifier));
    }

    /**
     * Parse a stringn integer value, or return the default value if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Integer parseIntD (byte[] qualifier, Integer defaultValue) {
        return parseIntD(family, qualifier, defaultValue);
    }

    /**
     * Parse a stringn integer value, or return the default value if the column is nonexistent.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Integer parseIntD (byte[] family, byte[] qualifier, Integer defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        String value = getStringD(family, qualifier, null);

        // the cast avoids an ugly auto-boxing bug when defaultValue is null
        return value != null ? (Integer) Integer.parseInt(value) : defaultValue;
    }

    /**
     * Parse a string long value, or throw an exception if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
     */
    public Long parseLong (byte[] qualifier)
    throws ModelException {
        return parseLong(family, qualifier);
    }

    /**
     * Parse a string long value, or throw an exception if the column is nonexistent.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
     */
    public Long parseLong (byte[] family, byte[] qualifier)
    throws ModelException {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        String value = getStringD(family, qualifier, null);

        if (value != null) {
            return Long.parseLong(value);
        }

        throw new ModelException(ERR_NONEXISTENT_COLUMN, Bytes.toString(family), Bytes.toString(qualifier));
    }

    /**
     * Parse a string long value, or return the default value if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Long parseLongD (byte[] qualifier, Long defaultValue) {
        return parseLongD(family, qualifier, defaultValue);
    }

    /**
     * Parse a string long value, or return the default value if the column is nonexistent.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Long parseLongD (byte[] family, byte[] qualifier, Long defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        String value = getStringD(family, qualifier, null);

        // the cast avoids an ugly auto-boxing bug when defaultValue is null
        return value != null ? (Long) Long.parseLong(value) : defaultValue;
    }

    /**
     * Parse a string short value, or throw an exception if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
     */
    public Short parseShort (byte[] qualifier)
    throws ModelException {
        return parseShort(family, qualifier);
    }

    /**
     * Parse a string short value, or throw an exception if the column is nonexistent.
     *
     * @param family    The column family.
     * @param qualifier The qualifier.
     *
     * @throws ModelException If the column is nonexistent.
     */
    public Short parseShort (byte[] family, byte[] qualifier)
    throws ModelException {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        String value = getStringD(family, qualifier, null);

        if (value != null) {
            return Short.parseShort(value);
        }

        throw new ModelException(ERR_NONEXISTENT_COLUMN, Bytes.toString(family), Bytes.toString(qualifier));
    }

    /**
     * Parse a string short value, or return the default value if the column is nonexistent.
     *
     * <p>
     * <strong>Note:</strong> This assumes {@link #setColumnFamily} has been called.
     * </p>
     *
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Short parseShortD (byte[] qualifier, Short defaultValue) {
        return parseShortD(family, qualifier, defaultValue);
    }

    /**
     * Parse a string short value, or return the default value if the column is nonexistent.
     *
     * @param family       The column family.
     * @param qualifier    The qualifier.
     * @param defaultValue The default value.
     */
    public Short parseShortD (byte[] family, byte[] qualifier, Short defaultValue) {
        assert family != null && qualifier != null
             : "family == null || qualifier == null";

        String value = getStringD(family, qualifier, null);

        // the cast avoids an ugly auto-boxing bug when defaultValue is null
        return value != null ? (Short) Short.parseShort(value) : defaultValue;
    }

    /**
     * Set the default column family.
     *
     * @param family The column family.
     */
    public Model setColumnFamily (byte[] family) {
        this.family = family;

        return this;
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

    /**
     * {@link CompareOp} represents a comparison operation.
     *
     * @author Sean Kerr [sean@code-box.org]
     */
    public enum CompareOp {
        /** The equality comparison. */
        EQ,

        /** The greater than comparison. */
        GT,

        /** The less than comparison. */
        LT
    }
}