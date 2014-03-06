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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
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

    /** The test table instance. */
    public HTableInterface table;

    /** The expected exception. */
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /**
     * Delete a row from the currently active table.
     *
     * @param row The row.
     */
    public void deleteRow (byte[] row)
    throws Exception {
        table.delete(new Delete(row));
        table.flushCommits();
    }

    /**
     * Flush the currently active table.
     */
    public void flushTable ()
    throws Exception {
        table.flushCommits();
    }

    /**
     * Retrieve a row from the currently active table.
     *
     * @param row The row.
     */
    public Result getRow (byte[] row)
    throws Exception {
        return table.get(new Get(row));
    }

    /**
     * Retrieve a table.
     *
     * @param table The table.
     */
    public HTableInterface getTable (byte[] table)
    throws Exception {
        return new HTable(config, table);
    }

    /**
     * Indicates whether or not a row is present in the currently active table.
     *
     * @param row The row.
     */
    public boolean hasRow (byte[] row)
    throws Exception {
        Result result = getRow(row);

        return result != null && !result.isEmpty();
    }

    /**
     * Initialize an individual test.
     */
    public void init ()
    throws Exception {
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
    }

    /**
     * Switch the currently active table.
     *
     * @param table The table.
     */
    public void switchTable (byte[] table)
    throws Exception {
        if (this.table != null) {
            this.table.close();
        }

        this.table = getTable(table);
    }

    /**
     * Write a put to the currently active table.
     *
     * @param put The put.
     */
    public void writePut(Put put)
    throws Exception {
        table.put(put);
    }
}