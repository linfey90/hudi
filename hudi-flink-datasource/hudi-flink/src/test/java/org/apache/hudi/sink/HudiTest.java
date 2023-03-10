/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.sink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
public class HudiTest {
  public HudiTest() {
  }

  public static void main(String[] args) {
    System.setProperty("HADOOP_USER_NAME", "root");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    //env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    env.setParallelism(1);
    StreamTableEnvironment tblEnv = StreamTableEnvironment.create(env);

    env.enableCheckpointing(1000 * 1000);
    // 1.创建Catalog
//    tblEnv.executeSql("CREATE CATALOG hoodie_catalog"
//            + "  WITH ("
//            + "    'type'='hudi',"
//            + "    'hive.conf.dir' = '/home/linfey/Downloads/hive-conf/node10/',"
//            + "    'mode'='hms'"
//            + "  )");

    tblEnv.executeSql("CREATE CATALOG hoodie_catalog" +
            "  WITH (" +
            "    'type'='hudi'," +
            " 'catalog.path'='s3a://test/hoodie_catalog'," +
            "    'hive.conf.dir' = '/home/ideaworkspace/deepexi/dlink-catalog-manager/catalog_manager/src/test/resources/'," +
            "    'mode'='hms'" +
            "  )");


    // 2.使用当前Catalog
    tblEnv.useCatalog("hoodie_catalog");

    // 3.创建数据库
//    tblEnv.executeSql("create database hudi_db");

    // 4.使用数据库
    tblEnv.useDatabase("hudi_db");

    ////     5.创建iceberg表
//        tblEnv.executeSql("CREATE TABLE hudi_table2(\n" +
//                "  id int PRIMARY KEY NOT ENFORCED, \n" +
//                "  name string\n" +
//                ")\n" +
//                "WITH (\n" +
//                "'connector' = 'hudi',\n" +
//                "'table.type' = 'COPY_ON_WRITE'" +
//                ")");

    //tblEnv.executeSql(String.format("select id,name from %s", tableName)).print();
    // 6.写入数据到表 flink_iceberg_tbl
    tblEnv.executeSql("insert into hudi_table values "
            + "(1,'aaa'),(2,'bbb'),(3,'ccc')");
    //        "(3,'ggg'),(4,'ddd'),(5,'fff')");


  }
}
