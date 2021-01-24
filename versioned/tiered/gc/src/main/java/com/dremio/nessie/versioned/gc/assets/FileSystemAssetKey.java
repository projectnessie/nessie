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
package com.dremio.nessie.versioned.gc.assets;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.dremio.nessie.versioned.gc.iceberg.IcebergAssetKeyReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.clearspring.analytics.util.Lists;
import com.dremio.nessie.versioned.AssetKey;
import com.dremio.nessie.versioned.gc.AssetKeyReader.AssetKeyType;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.util.SerializableConfiguration;

public class FileSystemAssetKey extends AssetKey implements Serializable {
  private static final Splitter DOT = Splitter.on(".");

  private String path;
  private SerializableConfiguration hadoopConfig;
  private AssetKeyType type;

  public FileSystemAssetKey() {

  }

  public FileSystemAssetKey(String path, SerializableConfiguration hadoopConfig, AssetKeyType type) {
    this.type = type;
    Preconditions.checkNotNull(path);
    this.path = path;
    this.hadoopConfig = hadoopConfig;
  }

  @Override
  public CompletionStage<Boolean> delete() {
    Path path = new Path(this.path);
    try {
      return CompletableFuture.completedFuture(path.getFileSystem(hadoopConfig.value()).delete(path, type.isRecursive()));
    } catch (IOException e) {
      throw new IllegalStateException(String.format("Unable to delete %s", this.path), e);
    }
  }

  @Override
  public List<String> toReportableName() {
    List<String> name = Lists.newArrayList();
    name.add(type.name());
    name.addAll(DOT.splitToList(path));
    return name;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FileSystemAssetKey that = (FileSystemAssetKey) o;
    return path.equals(that.path) && type == that.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, type);
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
