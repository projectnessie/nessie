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
package org.projectnessie.hms;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Hive2PartitionFilterer implements PartitionFilterer.PartitionFiltererImpl {

  private static Logger LOG = LoggerFactory.getLogger(Hive2PartitionFilterer.class);

  @Override
  public boolean filterPartitionsByExpr(
      Configuration conf, List<FieldSchema> partColumns, byte[] expr, List<String> partitionNames)
      throws MetaException {

    String defaultPartName = HiveConf.getVar(conf, ConfVars.DEFAULTPARTITIONNAME);
    PartitionExpressionProxy pems = createExpressionProxy(conf);
    return pems.filterPartitionsByExpr(
        partColumns.stream().map(FieldSchema::getName).collect(Collectors.toList()),
        partColumns.stream()
            .map(FieldSchema::getType)
            .map(TypeInfoFactory::getPrimitiveTypeInfo)
            .collect(Collectors.toList()),
        expr,
        defaultPartName,
        partitionNames);
  }

  @SuppressWarnings("unchecked")
  private static PartitionExpressionProxy createExpressionProxy(Configuration conf) {
    String className = HiveConf.getVar(conf, ConfVars.METASTORE_EXPRESSION_PROXY_CLASS);
    try {
      Class<? extends PartitionExpressionProxy> clazz = JavaUtils.loadClass(className);
      return clazz.newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      LOG.error("Error loading PartitionExpressionProxy", e);
      throw new RuntimeException("Error loading PartitionExpressionProxy: " + e.getMessage());
    }
  }
}
