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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Hive3PartitionFilterer implements PartitionFilterer.PartitionFiltererImpl {

  private static Logger LOG = LoggerFactory.getLogger(Hive3PartitionFilterer.class);

  @Override
  public boolean filterPartitionsByExpr(
      Configuration conf, List<FieldSchema> partColumns, byte[] expr, List<String> partitionNames)
      throws MetaException {

    String defaultPartName = MetastoreConf.getVar(conf, ConfVars.DEFAULTPARTITIONNAME);
    PartitionExpressionProxy pems = createExpressionProxy(conf);
    return pems.filterPartitionsByExpr(partColumns, expr, defaultPartName, partitionNames);
  }

  private static PartitionExpressionProxy createExpressionProxy(Configuration conf) {
    String className = MetastoreConf.getVar(conf, ConfVars.EXPRESSION_PROXY_CLASS);
    try {
      Class<? extends PartitionExpressionProxy> clazz =
          JavaUtils.getClass(className, PartitionExpressionProxy.class);
      return JavaUtils.newInstance(clazz, new Class<?>[0], new Object[0]);
    } catch (MetaException e) {
      LOG.error("Error loading PartitionExpressionProxy", e);
      throw new RuntimeException("Error loading PartitionExpressionProxy: " + e.getMessage());
    }
  }
}
