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
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.Rule;
import org.junit.rules.ExpectedException;

/**
 * @author Sean Kerr [sean@code-box.org]
 */
public abstract class BaseTest {
    /** A nonexistent value. */
    public static final byte[] NONEXISTENT = Bytes.toBytes("nonexistent");

    /** The test family. */
    public static final byte[] TEST_FAMILY = Bytes.toBytes("test");

    /** The test table. */
    public static final byte[] TEST_TABLE = Bytes.toBytes("simplebase_test");

    /** The configuration. */
    public static Configuration config;

    /** The test table instance. */
    public static HTableInterface table;

    /** The expected exception. */
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /** Initialize the test. */
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

        table = new HTable(config, TEST_TABLE);
    }
}