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
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Sean Kerr [sean@code-box.org]
 */
public class TestSetup {
    /** The test family and table. */
    public static final byte[] FAMILY1 = Bytes.toBytes("f1");
    public static final byte[] FAMILY2 = Bytes.toBytes("f2");
    public static final byte[] TABLE1  = Bytes.toBytes("simplebase_test1");
    public static final byte[] TABLE2  = Bytes.toBytes("simplebase_test2");

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

        for (byte[] table : new byte[][]{TABLE1, TABLE2}) {
            if (admin.tableExists(table)) {
                admin.disableTable(table);
                admin.deleteTable(table);
            }

            HTableDescriptor tableDescriptor  = new HTableDescriptor(table);

            for (byte[] family : new byte[][]{FAMILY1,FAMILY2}) {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);

                columnDescriptor.setMaxVersions(1);
                columnDescriptor.setMinVersions(1);
                tableDescriptor.addFamily(columnDescriptor);
            }

            admin.createTable(tableDescriptor);
        }
    }
}