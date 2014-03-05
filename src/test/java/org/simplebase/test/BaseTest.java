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

package org.simplebase.test;

import org.simplebase.model.Model;
import org.simplebase.writer.Writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.Rule;
import org.junit.rules.ExpectedException;

/**
 * @author Sean Kerr [sean@code-box.org]
 */
public abstract class BaseTest {
    /** A nonexistent value. */
    public static final byte[] NONEXISTENT = Bytes.toBytes("nonexistent");

    /** The test families. */
    public static final byte[] FAMILY1 = Bytes.toBytes("f1");
    public static final byte[] FAMILY2 = Bytes.toBytes("f2");

    /** The test tables. */
    public static final byte[] TABLE1 = Bytes.toBytes("simplebase_test1");
    public static final byte[] TABLE2 = Bytes.toBytes("simplebase_test2");

    /** The configuration. */
    public static Configuration config;

    /** The model. */
    public static Model model;

    /** The result. */
    public static Result result;

    /** The test table instance. */
    public static HTableInterface table;

    /** The writer. */
    public static Writer writer;

    /** The expected exception. */
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /**
     * Delete a row from the currently active table.
     *
     * @param row The row.
     */
    public static void deleteRow (byte[] row)
    throws Exception {
        table.delete(new Delete(row));
        table.flushCommits();
    }

    /**
     * Retrieve a model instance from the currently active table.
     *
     * @param row The row.
     */
    public static Model getModel (byte[] row)
    throws Exception {
        Result result = getRow(row);

        if (result == null) {
            throw new Exception("Nonexistent row: " + Bytes.toString(row));
        }

        return new Model(result);
    }

    /**
     * Retrieve a row from the currently active table.
     *
     * @param row The row.
     */
    public static Result getRow (byte[] row)
    throws Exception {
        return table.get(new Get(row));
    }

    /**
     * Retrieve a table.
     *
     * @param table The table.
     */
    public static HTableInterface getTable (byte[] table)
    throws Exception {
        return new HTable(config, table);
    }

    /**
     * Indicates whether or not a row is present in the currently active table.
     *
     * @param row The row.
     */
    public static boolean hasRow (byte[] row)
    throws Exception {
        Result result = getRow(row);

        return result != null && !result.isEmpty();
    }

    /**
     * Setup the test environment.
     */
    public static void setup ()
    throws Exception {
        if (config != null) {
            return;
        }

        config = HBaseConfiguration.create();

        if (System.getenv("HB_MASTER") != null) {
            config.set("hbase.master", System.getenv("HB_MASTER"));
        }

        if (System.getenv("ZK_QUORUM") != null) {
            config.set("hbase.zookeeper.quorum", System.getenv("ZK_QUORUM"));

            if (System.getenv("ZK_PORT") != null) {
                config.set("hbase.zookeeper.property.clientPort", System.getenv("ZK_PORT"));
            }
        }

        switchTable(TABLE1);
    }

    /**
     * Switch the currently active model.
     *
     * @param row The row.
     */
    public static void switchModel (byte[] row)
    throws Exception {
        model = getModel(row);
    }

    /**
     * Switch the currently active table.
     *
     * @param table The table.
     */
    public static void switchTable (byte[] table)
    throws Exception {
        if (BaseTest.table != null) {
            BaseTest.table.close();
        }

        BaseTest.table = getTable(table);
    }
}