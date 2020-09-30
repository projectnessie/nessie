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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseRawStore implements RawStoreWithRef {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  protected NessieStoreImpl nessie;
  protected RawStore delegate;
  protected boolean hasDelegate;
  protected Configuration conf;
  private RoutingTransactionHandler handler;
  private Set<String> nessieDbs = new HashSet<>();

  protected BaseRawStore(boolean hasDelegate) {
    this.nessie = new NessieStoreImpl();
    this.hasDelegate = hasDelegate;
    this.handler = new RoutingTransactionHandler(() -> nessie, () -> Optional.ofNullable(delegate));
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    if (nessie == null) {
      nessie = new NessieStoreImpl();
    }

    nessie.setConf(conf);
    if (hasDelegate) {
      String whitelistDbs = conf.get(NessieStore.NESSIE_WHITELIST_DBS_OPTION, "");
      nessieDbs = Stream.of(whitelistDbs.split(",")).map(String::trim).filter(s -> s.length() > 0).map(String::toLowerCase).collect(Collectors.toSet());
      if(nessieDbs.isEmpty()) {
        logger.warn("Using delegating Nessie store but no databases were routed to use Nessie functionality. Please update the {} configuration property in your Hive configuration.", NessieStore.NESSIE_WHITELIST_DBS_OPTION);
      } else {
        logger.debug("Configuring a delegating store where the following databases are considered Nessie databases: {}.", nessieDbs);
      }

      if (delegate == null) {
        delegate = new ObjectStore();
      }
      delegate.setConf(conf);
    }
  }

  @Override
  public String getRef() {
    return nessie.getRef();
  }

  protected static int union(int i, int x) {
    return i + x;
  }

  protected static <T> List<T> union(List<T> a, List<T> b) {
    return Stream.concat(a.stream(), b.stream()).collect(Collectors.toList());
  }

  protected static Stream<String> route(Database db) {
    return Stream.of(db.getName());
  }

  protected static Stream<String> route(Table table) {
    return Stream.of(table.getDbName());
  }

  protected static Stream<String> route(Partition partition) {
    return Stream.of(partition.getDbName());
  }

  protected static Stream<String> route(ColumnStatistics s) {
    return Stream.of(s.getStatsDesc().getDbName());
  }

  protected static Stream<String> route(Index i) {
    return Stream.of(i.getDbName());
  }

  protected static Stream<String> route(SQLForeignKey c) {
    return Stream.of(c.getFktable_db(), c.getPktable_db());
  }

  protected static Stream<String> route(SQLPrimaryKey c) {
    return Stream.of(c.getTable_db());
  }

  protected static Stream<String> route(String s) {
    return Stream.of(s);
  }

  private boolean isNessieDb(String name) {
    if ("$nessie".equalsIgnoreCase(name)) {
      return true;
    }

    if ("nessie".equalsIgnoreCase(name)) {
      return true;
    }

    return nessieDbs.contains(name.toLowerCase());
  }

  protected boolean routeToDelegate(Stream<String> databases) {
    boolean useDelegate = checkRouteToDelegate(databases);
    if (useDelegate) {
      handler.routedDelegate();
    } else {
      handler.routedNessie();
    }
    return useDelegate;
  }

  private boolean checkRouteToDelegate(Stream<String> databases) {
    if (!hasDelegate) {
      return false;
    }

    Set<String> dbs = databases.collect(Collectors.toSet());

    int nessieMatches = 0;
    for (String db : dbs) {
      if (isNessieDb(db)) {
        nessieMatches++;
      }
    }

    if (nessieMatches == 0) {
      return true;
    }

    if (nessieMatches == dbs.size()) {
      return false;
    }

    throw new IllegalArgumentException(
        "You tried to do an operation that referenced both Nessie and Legacy database types. This is not supported.");
  }


  protected void checkHasDelegate() {
    if (!hasDelegate) {
      throw new IllegalArgumentException("Nessie does not support this operation.");
    }
  }

  @Override
  public void shutdown() {
    if (nessie != null) {
      nessie.shutdown();
    }

    if (delegate != null) {
      delegate.shutdown();
    }
  }

  @Override
  public boolean openTransaction() {
    return handler.openTransaction();
  }

  @Override
  public boolean commitTransaction() {
    return handler.commitTransaction();
  }

  @Override
  public boolean isActiveTransaction() {
    return handler.isActiveTransaction();
  }

  @Override
  public void rollbackTransaction() {
    handler.rollbackTransaction();
  }

  /**
   * Return list of available catalogs.
   */
  public List<String> getCatalogs() throws MetaException {
    if (hasDelegate) {
      return delegate.getCatalogs();
    }

    return Collections.singletonList("hive");

  }

  /**
   * Metastore schema version report.
   */
  public String getMetaStoreSchemaVersion() throws MetaException {
    if (hasDelegate) {
      return delegate.getMetaStoreSchemaVersion();
    }
    return "nessie";
  }


  /**
   * Metastore uuid.
   */
  public String getMetastoreDbUuid() throws MetaException {
    if (hasDelegate) {
      return delegate.getMetastoreDbUuid();
    }

    return "nessie";
  }

}
