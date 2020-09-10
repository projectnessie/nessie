/*
 * Copyright (C) 2020 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.nessie.hms;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;

public final class PartitionFilterer {

  static final Class<?> CLAZZ;

  static {
    Class<?> clazz = null;
    try {
      clazz = Class.forName("com.dremio.nessie.hms.Hive2PartitionFilterer");
    } catch (Exception e) {
      clazz = Hive3PartitionFilterer.class;
    }
    CLAZZ = clazz;
  }

  private PartitionFilterer() {

  }

  public interface PartitionFiltererImpl {
    public abstract boolean filterPartitionsByExpr(
        Configuration conf,
        List<FieldSchema> partColumns,
        byte[] expr,
        List<String> partitionNames) throws MetaException;
  }


  /**
   * Filter the partitions.
   */
  public static boolean filterPartitionsByExpr(
      Configuration conf,
      List<FieldSchema> partColumns,
      byte[] expr,
      List<String> partitionNames) throws MetaException {
    try {
      return ((PartitionFiltererImpl) CLAZZ.newInstance()).filterPartitionsByExpr(conf, partColumns, expr, partitionNames);
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

}
