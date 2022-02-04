/*
 * Copyright (C) 2022 Dremio
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
package org.apache.iceberg.nessie;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewDefinition;
import org.apache.iceberg.view.Views;
import org.projectnessie.error.NessieNotFoundException;

/**
 * This is a wrapper catalog that supports both Iceberg Views and Tables. USE AT YOUR OWN RISK /
 * EXPERIMENTAL CODE.
 */
public class NessieExtCatalog extends BaseMetastoreCatalog implements Views {

  private NessieViewCatalog viewCatalog;
  private NessieCatalog tableCatalog;
  private Configuration configuration;

  @Override
  public void initialize(String name, Map<String, String> properties) {
    tableCatalog = new NessieCatalog();
    tableCatalog.setConf(configuration);
    tableCatalog.initialize(name, properties);
    viewCatalog = new NessieViewCatalog(configuration);
    viewCatalog.initialize("nessie_view_catalog", properties);
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return tableCatalog.newTableOps(tableIdentifier);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    return tableCatalog.defaultWarehouseLocation(tableIdentifier);
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    return tableCatalog.listTables(namespace);
  }

  @Override
  public boolean dropTable(TableIdentifier tableIdentifier, boolean purge) {
    return tableCatalog.dropTable(tableIdentifier, purge);
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    tableCatalog.renameTable(from, to);
  }

  @Override
  public void create(
      String viewIdentifier, ViewDefinition viewDefinition, Map<String, String> properties) {
    viewCatalog.create(viewIdentifier, viewDefinition, properties);
  }

  @Override
  public void replace(
      String viewIdentifier, ViewDefinition viewDefinition, Map<String, String> properties) {
    viewCatalog.replace(viewIdentifier, viewDefinition, properties);
  }

  @Override
  public View load(String viewIdentifier) {
    return viewCatalog.load(viewIdentifier);
  }

  @Override
  public ViewDefinition loadDefinition(String viewIdentifier) {
    return viewCatalog.loadDefinition(viewIdentifier);
  }

  @Override
  public void drop(String viewIdentifier) {
    viewCatalog.drop(viewIdentifier);
  }

  public void setConf(Configuration conf) {
    this.configuration = conf;
  }

  public void refresh() throws NessieNotFoundException {
    tableCatalog.refresh();
    viewCatalog.refresh();
  }

  public void close() {
    tableCatalog.close();
    viewCatalog.close();
  }

  @VisibleForTesting
  public NessieViewCatalog getViewCatalog() {
    return viewCatalog;
  }
}
