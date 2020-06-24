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

package com.dremio.nessie.deltalake;

import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.client.NessieClient.AuthType;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.ImmutableBranch;
import com.dremio.nessie.model.ImmutableTable;
import com.dremio.nessie.model.Table;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;

/**
 * How do we integrate this like we integrate iceberg.
 *
 * <p>This isn't fully functional yet!!!
 */
public class DeltaLake extends LogStoreWrapper {

  private static final BigInteger BI_2_64 = BigInteger.ONE.shiftLeft(64);

  private final SparkConf sparkConf;
  private final Configuration configuration;
  private final NessieClient client;
  private final AtomicReference<Branch> branch;


  /**
   * Construct the Nessie Delta Lake Log store.
   */
  public DeltaLake(SparkConf sparkConf, Configuration config) {
    this.sparkConf = sparkConf;
    this.configuration = config;
    String path = config.get("nessie.url");
    String username = config.get("nessie.username");
    String password = config.get("nessie.password");
    String authTypeStr = config.get("nessie.auth.type");
    AuthType authType = AuthType.valueOf(authTypeStr.toUpperCase());
    this.client = new NessieClient(authType, path, username, password);
    branch = new AtomicReference<>(getOrCreate(config.get("nessie.view-branch",
                                                          client.getConfig().getDefaultBranch())));
  }

  private Branch getOrCreate(String branchName) {
    Branch branch = client.getBranch(branchName);
    if (branch == null) {
      branch = client.createBranch(ImmutableBranch.builder().name(branchName).id("master").build());
    }
    return branch;
  }

  /**
   * Invalidate any caching that the implementation may be using.
   */
  public void invalidateCache() {

  }

  private String extractTableName(Path path) {
    return path.getParent().getParent().toUri().getPath();
  }

  /**
   * Read data from path.
   */
  @Override
  public List<String> readImpl(Path path) throws IOException {
    FileSystem fs = path.getFileSystem(configuration);
    // todo don't read if it a path is greater than the current pointer
    Table table = client.getTable(branch.get().getName(),
                                  path.getParent().getParent().toUri().getPath(),
                                  null);
    if (table == null) {
      throw new FileNotFoundException(path.toString() + " does not exist");
    }
    Path metadataPath = new Path(table.getMetadataLocation());
    String extension = FilenameUtils.getExtension(metadataPath.getName());
    String requestedExtension = FilenameUtils.getExtension(path.getName());
    Path requestedPath = new Path(metadataPath.getParent(),
                                  metadataPath.getName().replace(extension, requestedExtension));
    try (FSDataInputStream stream = fs.open(requestedPath)) {
      BufferedReader reader =
        new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
      return IOUtils.readLines(reader).stream().map(String::trim).collect(Collectors.toList());
    }
  }

  private Path unConvertPath(Path path) {
    String originalFilename = path.getName();
    String extension = "." + FilenameUtils.getExtension(originalFilename);
    String file = originalFilename.replace(extension, "");

    return new Path(path.getParent(), file.substring(0, 20) + extension);
  }

  private Path convertPath(Path path) {
    String originalFilename = path.getName();
    String extension = "." + FilenameUtils.getExtension(originalFilename);
    String file = originalFilename.replace(extension, "");
    Long fileId = Long.parseLong(file);
    long uuid = ThreadLocalRandom.current().nextLong(1_000L);

    long newFileId = uuid;
    if (newFileId > (long) Integer.MAX_VALUE) {
      throw new UnsupportedOperationException("Too many snapshots! " + newFileId);
    }
    return new Path(path.getParent(), String.format("%020d", newFileId) + extension);
  }

  private Path convertPathForMaster(Path path, Path originalPath) {
    String originalFilename = originalPath.getName();
    String extension = "." + FilenameUtils.getExtension(originalFilename);
    String file = originalFilename.replace(extension, "");
    Long fileId = Long.parseLong(file) + 1;
    String newFileId = String.format("%08d%012d", fileId, 0L);

    return new Path(path.getParent(), newFileId + extension);
  }

  private void doWrite(Path path, FileSystem fs, List<String> actions, boolean isMetadata)
    throws IOException {

    Path newPath = convertPath(path);

    try (FSDataOutputStream stream = fs.create(newPath)) {
      for (String s : actions) {
        String s1 = s + "\n";
        byte[] bytes = s1.getBytes(StandardCharsets.UTF_8);
        stream.write(bytes);
      }
    }
    if (isMetadata) {
      try {
        String tableName = extractTableName(newPath);
        Table table = client.getTable(branch.get().getName(), tableName, null);
        if (table == null) {
          table = ImmutableTable.builder()
                                .id(tableName)
                                .name(tableName)
                                .metadataLocation(newPath.toString())
                                //todo metadata
                                .build();
        } else {
          table = ImmutableTable.builder()
                                .from(table)
                                .metadataLocation(newPath.getName())
                                //todo metadata
                                .build();
        }
        client.commit(branch.get(), table);
//        if (branch.get().getName().equals("master")) {
//          try (FSDataOutputStream masterStream = fs.create(convertPathForMaster(newPath, path))) {
//            for (String s : actions) {
//              String s1 = s + "\n";
//              byte[] bytes = s1.getBytes(StandardCharsets.UTF_8);
//              masterStream.write(bytes);
//            }
//          }
//        }
      } catch (RuntimeException e) {
        throw new FileNotFoundException(path.toString());
      }
    }
  }

  /**
   * Write data to path.
   */
  @Override
  public void writeImpl(Path path, List<String> actions, boolean overwrite) throws IOException {
    FileSystem fs = path.getFileSystem(configuration);

    if (!fs.exists(path.getParent())) {
      throw new FileNotFoundException("No such file or directory: ${path.getParent}");
    }
    boolean isMetadata = !path.toString().endsWith("crc")
                         && !path.toString().endsWith("checkpoint");
    doWrite(path, fs, actions, isMetadata);
  }

  /**
   * List all data at this path.
   */
  @Override
  public List<FileStatus> listFromImpl(Path path) throws IOException {
    // todo remove from this list any file that is greater than the current pointer
    Table table = client.getTable(branch.get().getName(),
                                  path.getParent().getParent().toUri().getPath(),
                                  null);
    if (table == null) {
      throw new FileNotFoundException(path.toString() + " does not exist");
    }
    FileSystem fs = path.getFileSystem(configuration);
    if (!fs.exists(path.getParent())) {
      throw new FileNotFoundException("No such file or directory: ${path.getParent}");
    }
    FileStatus[] files = fs.listStatus(path.getParent());
    List<FileStatus> collect = new ArrayList<>();
    for (FileStatus file : files) {
      String filename = file.getPath().getName();
      String currentMetadataName = new Path(table.getMetadataLocation()).getName();
      if (filename.equals(currentMetadataName)) {
        file.setPath(file.getPath());
        collect.add(file);
      }
    }
    if (collect.size() == 1) {
      path = collect.get(0).getPath();
      String extension = "." + FilenameUtils.getExtension(path.getName());
      String file = path.getName().replace(extension, "");
      int endId = Math.toIntExact(Long.parseLong(file));
      List<FileStatus> statuses = getAll(endId, path.getParent(), FilenameUtils.getExtension(path.getName()));
      statuses.add(endId, collect.get(0));
      return statuses;
    }
    collect.sort(Comparator.comparing(p -> p.getPath().getName()));
    return collect;
  }

  private static List<FileStatus> getAll(int endId, Path parent, String extension) {
    List<FileStatus> results = new ArrayList<FileStatus>(endId+1);
    for (int i = 0; i < endId; i++) {
      results.add(i, get(i, parent, extension));
    }
    return results;
  }

  private static FileStatus get(long id, Path parent, String extension) {
    Path path = new Path(parent, String.format("%020d.%s", id, extension));
    return new FileStatus(1, false, 0, 0, 0, path);
  }
}
