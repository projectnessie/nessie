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

package com.dremio.iceberg.deltalake;

import com.dremio.iceberg.client.AlleyClient;
import com.dremio.iceberg.model.Table;
import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;

/**
 * How do we integrate this like we integrate iceberg
 * No delete or
 */
public class DeltaLake extends LogStoreWrapper {

  private final SparkConf sparkConf;
  private final Configuration configuration;
  private final String baseDirectory;
  private final AlleyClient client;

  public DeltaLake(SparkConf sparkConf, Configuration configuration) {
    this.sparkConf = sparkConf;
    this.configuration = configuration;
    // base dir should come from config
    this.baseDirectory = "/home/ryan/workspace/iceberg/python/";
    this.client = new AlleyClient(configuration);
  }

  /**
   * Invalidate any caching that the implementation may be using
   */
  public void invalidateCache() {

  }

  private String extractTableName(Path path) {
    String[] pathParts = path.getName().replace(baseDirectory, "").split("_delta_log");
    return pathParts[0].replace("/", "");
  }

  public List<String> readImpl(Path path) throws IOException {
    FileSystem fs = path.getFileSystem(configuration);
    // todo don't read if it a path is greater than the current pointer
    try (FSDataInputStream stream = fs.open(path)) {
      BufferedReader reader =
        new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
      return IOUtils.readLines(reader).stream().map(String::trim).collect(Collectors.toList());
    }
  }

  private void overwrite(Path path, FileSystem fs, List<String> actions, boolean isMetadata)
    throws IOException {
    //todo this isn't safe
    try (FSDataOutputStream stream = fs.create(path, true)) {
      for (String x : actions) {
        String str = x + "\n";
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        stream.write(bytes);
      }
    }

    if (isMetadata) {
      for (int i = 0; i < 5; i++) {
        try {
          Table table =
            client.getTableClient().getObjectByName(extractTableName(path), null); //todo namespace
          table.setMetadataLocation(path.toString());
          client.getTableClient().updateObject(table);
        } catch (RuntimeException e) {
          // pass
        }
      }
    }
  }

  private void noOverwrite(Path path, FileSystem fs, List<String> actions, boolean isMetadata)
    throws IOException {
    //todo this isn't safe
    if (fs.exists(path)) {
      throw new FileAlreadyExistsException(path.toString());
    }
    boolean streamClosed = false; // This flag is to avoid double close
    boolean renameDone = false; // This flag is to save the delete operation in most of cases.
    FSDataOutputStream stream = fs.create(path);
    try {
      for (String s : actions) {
        String s1 = s + "\n";
        byte[] bytes = s1.getBytes(StandardCharsets.UTF_8);
        stream.write(bytes);
      }
      stream.close();
      streamClosed = true;
      if (isMetadata) {
        try {
          Table table =
            client.getTableClient().getObjectByName(extractTableName(path), null); //todo namespace
          table.setMetadataLocation(path.toString());
          client.getTableClient().updateObject(table);
          renameDone = true;
        } catch (RuntimeException e) {
          throw new FileNotFoundException(path.toString());
        }
      }
    } finally {
      if (!streamClosed) {
        stream.close();
      }
      if (!renameDone) {
        fs.delete(path, false);
      }
    }
  }

  public void writeImpl(Path path, List<String> actions, boolean overwrite) throws IOException {
    FileSystem fs = path.getFileSystem(configuration);

    if (!fs.exists(path.getParent())) {
      throw new FileNotFoundException("No such file or directory: ${path.getParent}");
    }
    boolean isMetadata =
      !path.toString().endsWith("crc") && !path.toString().endsWith("checkpoint");
    if (overwrite) {
      overwrite(path, fs, actions, isMetadata);
    } else {
      noOverwrite(path, fs, actions, isMetadata);
    }
  }

  public List<FileStatus> listFromImpl(Path path) throws IOException {
    // todo remove from this list any file that is greater than the current pointer
    FileSystem fs = path.getFileSystem(configuration);
    if (!fs.exists(path.getParent())) {
      throw new FileNotFoundException("No such file or directory: ${path.getParent}");
    }
    FileStatus[] files = fs.listStatus(path.getParent());
    List<FileStatus> collect = Lists.newArrayList();
    for (FileStatus file : files) {
      if (file.getPath().getName().compareTo(path.getName()) >= 0) {
        collect.add(file);
      }
    }
    collect.sort(Comparator.comparing(p -> p.getPath().getName()));
    return collect;
  }

}
