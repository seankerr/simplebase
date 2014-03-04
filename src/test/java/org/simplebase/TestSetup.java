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
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Sean Kerr [sean@code-box.org]
 */
public class TestSetup {
    /** The test family and table. */
    public static final byte[] TEST_FAMILY = Bytes.toBytes("test");
    public static final byte[] TEST_TABLE  = Bytes.toBytes("simplebase_test");

    /** Run setup. */
    public static void main (String[] args)
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

        HBaseAdmin admin = new HBaseAdmin(config);

        if (!admin.tableExists(TEST_TABLE)) {
            HTableDescriptor tableDescriptor   = new HTableDescriptor(TEST_TABLE);
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(TEST_FAMILY);

            columnDescriptor.setMaxVersions(1);
            columnDescriptor.setMinVersions(1);
            tableDescriptor.addFamily(columnDescriptor);

            admin.createTable(tableDescriptor);
        }

        HTable table = new HTable(config, TEST_TABLE);

        // model test data
        table.delete(new Delete(Bytes.toBytes("model_test")));

        Put put = new Put(Bytes.toBytes("model_test"));
        put.add(TEST_FAMILY, Bytes.toBytes("boolean1"),     Bytes.toBytes(true));
        put.add(TEST_FAMILY, Bytes.toBytes("boolean2"),     Bytes.toBytes(false));
        put.add(TEST_FAMILY, Bytes.toBytes("double1"),      Bytes.toBytes((double) 1.0));
        put.add(TEST_FAMILY, Bytes.toBytes("double2"),      Bytes.toBytes((double) 2.0));
        put.add(TEST_FAMILY, Bytes.toBytes("float1"),       Bytes.toBytes((float) 1.0));
        put.add(TEST_FAMILY, Bytes.toBytes("float2"),       Bytes.toBytes((float) 2.0));
        put.add(TEST_FAMILY, Bytes.toBytes("int1"),         Bytes.toBytes((int) 1));
        put.add(TEST_FAMILY, Bytes.toBytes("int2"),         Bytes.toBytes((int) 2));
        put.add(TEST_FAMILY, Bytes.toBytes("long1"),        Bytes.toBytes((long) 1));
        put.add(TEST_FAMILY, Bytes.toBytes("long2"),        Bytes.toBytes((long) 2));
        put.add(TEST_FAMILY, Bytes.toBytes("short1"),       Bytes.toBytes((short) 1));
        put.add(TEST_FAMILY, Bytes.toBytes("short2"),       Bytes.toBytes((short) 2));
        put.add(TEST_FAMILY, Bytes.toBytes("string1"),      Bytes.toBytes("hbase"));
        put.add(TEST_FAMILY, Bytes.toBytes("string2"),      Bytes.toBytes("rocks"));
        put.add(TEST_FAMILY, Bytes.toBytes("boolean_str1"), Bytes.toBytes("true"));
        put.add(TEST_FAMILY, Bytes.toBytes("boolean_str2"), Bytes.toBytes("false"));
        put.add(TEST_FAMILY, Bytes.toBytes("double_str1"),  Bytes.toBytes("1.0"));
        put.add(TEST_FAMILY, Bytes.toBytes("double_str2"),  Bytes.toBytes("2.0"));
        put.add(TEST_FAMILY, Bytes.toBytes("float_str1"),   Bytes.toBytes("1.0"));
        put.add(TEST_FAMILY, Bytes.toBytes("float_str2"),   Bytes.toBytes("2.0"));
        put.add(TEST_FAMILY, Bytes.toBytes("int_str1"),     Bytes.toBytes("1"));
        put.add(TEST_FAMILY, Bytes.toBytes("int_str2"),     Bytes.toBytes("2"));
        put.add(TEST_FAMILY, Bytes.toBytes("long_str1"),    Bytes.toBytes("1"));
        put.add(TEST_FAMILY, Bytes.toBytes("long_str2"),    Bytes.toBytes("2"));
        put.add(TEST_FAMILY, Bytes.toBytes("short_str1"),   Bytes.toBytes("1"));
        put.add(TEST_FAMILY, Bytes.toBytes("short_str2"),   Bytes.toBytes("2"));

        table.put(put);
    }
}