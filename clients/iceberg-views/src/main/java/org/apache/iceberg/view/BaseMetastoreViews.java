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
package org.apache.iceberg.view;

import static org.apache.iceberg.view.ViewUtils.toCatalogTableIdentifier;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NotFoundException;

public abstract class BaseMetastoreViews implements Views {
  private final Configuration conf;

  public BaseMetastoreViews(Configuration conf) {
    this.conf = conf;
  }

  protected abstract BaseMetastoreViewOperations newViewOps(TableIdentifier viewName);

  protected String defaultWarehouseLocation(TableIdentifier viewIdentifier) {
    throw new UnsupportedOperationException(
        "Implementation for 'defaultWarehouseLocation' not provided.");
  }

  @Override
  public void create(
      String viewIdentifier, ViewDefinition definition, Map<String, String> properties) {
    TableIdentifier viewName = toCatalogTableIdentifier(viewIdentifier);
    ViewOperations ops = newViewOps(viewName);
    if (ops.current() != null) {
      throw new AlreadyExistsException("View already exists: %s", viewName);
    }

    String location = defaultWarehouseLocation(viewName);
    int parentId = -1;

    ViewUtils.doCommit(
        DDLOperations.CREATE, properties, 1, parentId, definition, location, ops, null);
  }

  @Override
  public void replace(
      String viewIdentifier, ViewDefinition definition, Map<String, String> properties) {
    TableIdentifier viewName = toCatalogTableIdentifier(viewIdentifier);
    ViewOperations ops = newViewOps(viewName);
    if (ops.current() == null) {
      throw new AlreadyExistsException("View %s is expected to exist", viewName);
    }

    ViewVersionMetadata prevViewVersionMetadata = ops.current();
    Preconditions.checkState(
        prevViewVersionMetadata.versions().size() > 0, "Version history not found");
    int parentId = prevViewVersionMetadata.currentVersionId();

    String location = prevViewVersionMetadata.location();

    ViewUtils.doCommit(
        DDLOperations.REPLACE,
        properties,
        parentId + 1,
        parentId,
        definition,
        location,
        ops,
        prevViewVersionMetadata);
  }

  @Override
  public void drop(String viewIdentifier) {
    TableIdentifier viewName = toCatalogTableIdentifier(viewIdentifier);
    ViewOperations ops = newViewOps(viewName);
    ops.drop(viewName.toString());
  }

  @Override
  public View load(String viewIdentifier) {
    TableIdentifier viewName = toCatalogTableIdentifier(viewIdentifier);
    ViewOperations ops = newViewOps(viewName);
    if (ops.current() == null) {
      throw new NotFoundException("View does not exist: %s", viewName);
    }
    return new BaseView(ops, viewName.toString());
  }

  @Override
  public ViewDefinition loadDefinition(String viewIdentifier) {
    TableIdentifier viewName = toCatalogTableIdentifier(viewIdentifier);
    ViewOperations ops = newViewOps(viewName);
    if (ops.current() == null) {
      throw new NotFoundException("View does not exist: %s", viewName);
    }
    return ops.current().definition();
  }
}
