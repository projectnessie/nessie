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
package org.projectnessie.versioned.gc;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.spark.util.SerializableConfiguration;

/**
 * Specialization of AssetKey to denote a file on a disk/store.
 *
 * <p>Uses hadoop filesystem to access the data, needs a valid hadoop config to work.
 */
public class IcebergAssetKey extends AssetKey implements Serializable {
  private static final Splitter SLASH = Splitter.on("/");

  public enum AssetKeyType {
    TABLE,
    ICEBERG_MANIFEST,
    ICEBERG_MANIFEST_LIST,
    ICEBERG_METADATA,
    DATA_FILE
  }

  private String path;
  private SerializableConfiguration hadoopConfig;
  private AssetKeyType type;
  private String snapshotId;
  private String tableName;

  public IcebergAssetKey() {}

  /** all args constructor for a filesystem asset key. */
  public IcebergAssetKey(
      String path,
      SerializableConfiguration hadoopConfig,
      AssetKeyType type,
      long snapshotId,
      String tableName) {
    this.type = type;
    this.snapshotId = Long.toString(snapshotId);
    this.tableName = tableName;
    Preconditions.checkNotNull(path);
    this.path = path;
    this.hadoopConfig = hadoopConfig;
  }

  @Override
  public CompletionStage<Boolean> delete() {
    Path path = new Path(this.path);
    try {
      return CompletableFuture.completedFuture(
          path.getFileSystem(hadoopConfig.value()).delete(path, AssetKeyType.TABLE.equals(type)));
    } catch (IOException e) {
      throw new IllegalStateException(String.format("Unable to delete %s", this.path), e);
    }
  }

  @Override
  public List<String> toReportableName() {
    List<String> name = Lists.newArrayList();
    name.add(type.name());
    name.add(snapshotId);
    name.add(tableName);
    try {
      name.addAll(
          SLASH.splitToList(new Path(path).toUri().toURL().getPath()).stream()
              .filter(x -> !x.isEmpty())
              .collect(Collectors.toList()));
    } catch (MalformedURLException e) {
      throw new RuntimeException(String.format("Cannot get name of path %s", path), e);
    }
    return name;
  }

  @Override
  public ByteString toUniqueKey() {
    return ByteString.copyFromUtf8(path);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IcebergAssetKey that = (IcebergAssetKey) o;
    return path.equals(that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path);
  }

  public String getSnapshotId() {
    return snapshotId;
  }

  public void setSnapshotId(String snapshotId) {
    this.snapshotId = snapshotId;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public AssetKeyType getType() {
    return type;
  }

  public void setType(AssetKeyType type) {
    this.type = type;
  }

  public void setHadoopConf(SerializableConfiguration configuration) {
    this.hadoopConfig = configuration;
  }
}
