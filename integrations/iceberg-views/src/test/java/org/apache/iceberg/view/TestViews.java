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
import com.google.common.collect.Maps;
import java.io.File;
import java.util.Map;
import org.apache.iceberg.Files;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

public class TestViews {

  private TestViews() {}

  public static void create(
      File temp, String name, ViewDefinition viewDefinition, Map<String, String> properties) {
    TestViewOperations ops = new TestViewOperations(name, temp);
    if (ops.current() != null) {
      throw new AlreadyExistsException("View %s already exists at location: %s", name, temp);
    }
    int parentId = -1;
    properties.put(CommonViewConstants.GENIE_ID, "test genie-id");
    properties.put(CommonViewConstants.ENGINE_VERSION, "TestEngine");

    ViewUtils.doCommit(
        DDLOperations.CREATE, properties, 1, parentId, viewDefinition, "", ops, null);
  }

  public static ViewVersionMetadata loadViewVersionMetadata(File temp, String name) {
    TestViewOperations ops = new TestViewOperations(name, temp);
    if (ops.current() != null) {
      return ops.current();
    }
    throw new NotFoundException("View '%s' not found", name);
  }

  public static BaseView load(File temp, String name) {
    TestViewOperations ops = new TestViewOperations(name, temp);
    return new BaseView(ops, name);
  }

  public static void replace(
      File temp, String name, ViewDefinition viewDefinition, Map<String, String> properties) {
    ViewVersionMetadata prevViewVersionMetadata = loadViewVersionMetadata(temp, name);
    TestViewOperations ops = new TestViewOperations(name, temp);
    if (ops.current() == null) {
      throw new NotFoundException("View %s does not exist at location: %s", name, temp);
    }

    Preconditions.checkState(
        prevViewVersionMetadata.versions().size() > 0, "Version history not found");
    int parentId = prevViewVersionMetadata.currentVersionId();

    String location = prevViewVersionMetadata.location();

    properties.put(CommonViewConstants.GENIE_ID, "test genie-id");
    properties.put(CommonViewConstants.ENGINE_VERSION, "TestEngine");
    ViewUtils.doCommit(
        DDLOperations.REPLACE,
        properties,
        parentId + 1,
        parentId,
        viewDefinition,
        location,
        ops,
        prevViewVersionMetadata);
  }

  public static void drop(String name, File temp) {
    TestViewOperations ops = new TestViewOperations(name, temp);
    if (ops.current() != null) {
      ops.drop(name);
      return;
    }
    throw new NotFoundException("View '%s' not found", name);
  }

  private static final Map<String, ViewVersionMetadata> METADATA = Maps.newHashMap();
  private static final Map<String, Integer> VERSIONS = Maps.newHashMap();

  static void clearTables() {
    synchronized (METADATA) {
      METADATA.clear();
      VERSIONS.clear();
    }
  }

  static ViewVersionMetadata readMetadata(String viewName) {
    synchronized (METADATA) {
      return METADATA.get(viewName);
    }
  }

  static Integer metadataVersion(String viewName) {
    synchronized (METADATA) {
      return VERSIONS.get(viewName);
    }
  }

  public static class TestViewOperations implements ViewOperations {

    private final String viewName;
    private final File metadata;
    private ViewVersionMetadata current = null;
    private long lastVersionId = 0;

    public TestViewOperations(String viewName, File location) {
      this.viewName = viewName;
      this.metadata = new File(location, "metadata");
      metadata.mkdirs();
      refresh();
      if (current != null) {
        for (Version version : current.versions()) {
          this.lastVersionId = Math.max(lastVersionId, version.versionId());
        }
      } else {
        this.lastVersionId = 0;
      }
    }

    @Override
    public ViewVersionMetadata current() {
      return current;
    }

    @Override
    public ViewVersionMetadata refresh() {
      synchronized (METADATA) {
        this.current = METADATA.get(viewName);
      }
      return current;
    }

    @Override
    public void commit(
        ViewVersionMetadata base,
        ViewVersionMetadata updatedMetadata,
        Map<String, String> properties) {
      if (base != current) {
        throw new CommitFailedException("Cannot commit changes based on stale metadata");
      }
      synchronized (METADATA) {
        refresh();
        if (base == current) {
          Integer version = VERSIONS.get(viewName);
          VERSIONS.put(viewName, version == null ? 0 : version + 1);
          METADATA.put(viewName, updatedMetadata);
          this.current = updatedMetadata;
        } else {
          throw new CommitFailedException(
              "Commit failed: table was updated at %d", current.currentVersion().timestampMillis());
        }
      }
    }

    @Override
    public void drop(String name) {
      synchronized (METADATA) {
        METADATA.remove(name);
        VERSIONS.remove(name);
      }
    }

    @Override
    public FileIO io() {
      return new LocalFileIO();
    }
  }

  static class LocalFileIO implements FileIO {

    @Override
    public InputFile newInputFile(String path) {
      return Files.localInput(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return Files.localOutput(path);
    }

    @Override
    public void deleteFile(String path) {
      if (!new File(path).delete()) {
        throw new RuntimeIOException("Failed to delete file: %s", path);
      }
    }
  }
}
