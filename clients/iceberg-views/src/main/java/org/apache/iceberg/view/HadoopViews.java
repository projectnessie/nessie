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

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;

/** The Views implementation Based on FileIO. */
public class HadoopViews implements Views, Configurable {
  private Configuration conf;

  public HadoopViews(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Loads the View object from a FileSystem path location.
   *
   * @param location a path URI (e.g. hdfs:///warehouse/my_view/)
   * @return view implementation
   */
  @Override
  public View load(String location) {
    ViewOperations ops = newViewOps(location);
    if (ops.current() == null) {
      throw new NoSuchTableException("View does not exist at location: %s", location);
    }

    return new BaseView(ops, location);
  }

  /** Loads the view definition from a FileSystem path location. */
  @Override
  public ViewDefinition loadDefinition(String location) {
    ViewOperations ops = newViewOps(location);
    if (ops.current() == null) {
      throw new NoSuchTableException("View does not exist at location: %s", location);
    }
    return ops.current().definition();
  }

  /**
   * Create a view using the FileSystem implementation, resolved from location.
   *
   * @param location a path URI (e.g. hdfs:///warehouse/my_view)
   * @param viewDefinition contains the view schema, sql, session catalog/namespace etc
   */
  @Override
  public void create(
      String location, ViewDefinition viewDefinition, Map<String, String> properties) {
    ViewOperations ops = newViewOps(location);
    if (ops.current() != null) {
      throw new AlreadyExistsException("View already exists at location: %s", location);
    }

    int parentId = -1;

    ViewUtils.doCommit(
        DDLOperations.CREATE, properties, 1, parentId, viewDefinition, location, ops, null);
  }

  @Override
  public void replace(String location, ViewDefinition definition, Map<String, String> properties) {
    ViewOperations ops = newViewOps(location);
    if (ops.current() == null) {
      throw new AlreadyExistsException("View is expected to exist at location: %s", location);
    }

    ViewVersionMetadata prevViewVersionMetadata = ops.current();
    Preconditions.checkState(
        prevViewVersionMetadata.versions().size() > 0, "Version history not found");
    int parentId = prevViewVersionMetadata.currentVersionId();

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
  public void drop(String location) {
    ViewOperations ops = newViewOps(location);
    ops.drop(location);
  }

  private ViewOperations newViewOps(String location) {
    return new HadoopViewOperations(new Path(location), conf);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
