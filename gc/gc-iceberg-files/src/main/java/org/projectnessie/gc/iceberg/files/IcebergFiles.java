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
package org.projectnessie.gc.iceberg.files;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.MustBeClosed;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.immutables.value.Value;
import org.projectnessie.gc.files.DeleteResult;
import org.projectnessie.gc.files.DeleteSummary;
import org.projectnessie.gc.files.FileDeleter;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.gc.files.FilesLister;
import org.projectnessie.gc.files.NessieFileIOException;
import org.projectnessie.storage.uri.StorageUri;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides functionality to {@link FilesLister list} and {@link FileDeleter delete} files using
 * Iceberg's {@link S3FileIO} for S3 schemes and/or {@link ResolvingFileIO} for non-S3 schemes.
 *
 * <p>The {@link FileIO} instances are only instantiated when needed.
 */
@Value.Immutable
public abstract class IcebergFiles implements FilesLister, FileDeleter, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergFiles.class);

  public static Builder builder() {
    return ImmutableIcebergFiles.builder();
  }

  static Stream<String> filesAsStrings(Stream<FileReference> fileObjects) {
    return fileObjects.map(FileReference::absolutePath).map(StorageUri::location);
  }

  public interface Builder {
    @CanIgnoreReturnValue
    Builder hadoopConfiguration(Configuration hadoopConfiguration);

    @CanIgnoreReturnValue
    Builder putProperties(String key, String value);

    @CanIgnoreReturnValue
    Builder putAllProperties(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder properties(Map<String, ? extends String> entries);

    IcebergFiles build();
  }

  @Value.Default
  Configuration hadoopConfiguration() {
    return new Configuration();
  }

  abstract Map<String, String> properties();

  @SuppressWarnings("immutables:incompat")
  private volatile boolean hasResolvingFileIO;

  @Value.Lazy
  public FileIO resolvingFileIO() {
    ResolvingFileIO fileIO = new ResolvingFileIO();
    fileIO.initialize(properties());
    fileIO.setConf(hadoopConfiguration());
    hasResolvingFileIO = true;
    LOGGER.debug("Instantiated Iceberg's ResolvingFileIO");
    return fileIO;
  }

  @Override
  public void close() {
    if (hasResolvingFileIO) {
      resolvingFileIO().close();
    }
  }

  private boolean supportsBulkAndPrefixOperations(StorageUri uri) {
    switch (uri.scheme()) {
      case "s3":
      case "s3a":
      case "s3n":
      case "gs":
      case "abfs":
      case "abfss":
        return true;
      default:
        return false;
    }
  }

  @Override
  @MustBeClosed
  public Stream<FileReference> listRecursively(StorageUri path) throws NessieFileIOException {
    StorageUri basePath = path.withTrailingSeparator();
    if (supportsBulkAndPrefixOperations(path)) {

      @SuppressWarnings("resource")
      SupportsPrefixOperations fileIo = (SupportsPrefixOperations) resolvingFileIO();
      Iterable<FileInfo> fileInfos;
      try {
        fileInfos = fileIo.listPrefix(basePath.toString());
      } catch (Exception e) {
        throw new NessieFileIOException("Failed to list prefix of " + path, e);
      }
      return StreamSupport.stream(fileInfos.spliterator(), false)
          .map(
              f -> {
                StorageUri location = StorageUri.of(f.location());
                if (!location.isAbsolute()) {
                  location = basePath.resolve("/").resolve(location);
                }
                return FileReference.of(
                    basePath.relativize(location), basePath, f.createdAtMillis());
              });
    }

    return listHadoop(basePath);
  }

  private Stream<FileReference> listHadoop(StorageUri basePath) throws NessieFileIOException {
    Path p = new Path(basePath.location());
    FileSystem fs;
    try {
      fs = p.getFileSystem(hadoopConfiguration());
    } catch (IOException e) {
      throw new NessieFileIOException("Failed to get Hadoop file system " + basePath, e);
    }

    return StreamSupport.stream(
        new AbstractSpliterator<>(Long.MAX_VALUE, 0) {
          private RemoteIterator<LocatedFileStatus> iterator;

          @Override
          public boolean tryAdvance(Consumer<? super FileReference> action) {
            try {
              if (iterator == null) {
                iterator = fs.listFiles(p, true);
              }

              if (!iterator.hasNext()) {
                return false;
              }

              LocatedFileStatus status = iterator.next();

              if (status.isFile()) {
                action.accept(
                    FileReference.of(
                        basePath.relativize(StorageUri.of(status.getPath().toUri())),
                        basePath,
                        status.getModificationTime()));
              }

              return true;
            } catch (IOException e) {
              throw new RuntimeException("Failed to list (via Hadoop) " + basePath, e);
            }
          }
        },
        false);
  }

  @Override
  public DeleteResult delete(FileReference fileReference) {
    try {
      StorageUri absolutePath = fileReference.absolutePath();
      @SuppressWarnings("resource")
      FileIO fileIO = resolvingFileIO();
      fileIO.deleteFile(absolutePath.toString());
      return DeleteResult.SUCCESS;
    } catch (Exception e) {
      LOGGER.debug("Failed to delete {}", fileReference, e);
      return DeleteResult.FAILURE;
    }
  }

  @Override
  public DeleteSummary deleteMultiple(StorageUri baseUri, Stream<FileReference> fileObjects) {
    Stream<String> filesAsStrings = filesAsStrings(fileObjects);

    if (supportsBulkAndPrefixOperations(baseUri)) {
      return s3DeleteMultiple(filesAsStrings);
    }
    return hadoopDeleteMultiple(filesAsStrings);
  }

  private DeleteSummary s3DeleteMultiple(Stream<String> filesAsStrings) {
    @SuppressWarnings("resource")
    SupportsBulkOperations fileIo = (SupportsBulkOperations) resolvingFileIO();

    List<String> files = filesAsStrings.collect(Collectors.toList());
    long failed = 0L;
    try {
      fileIo.deleteFiles(files);
    } catch (BulkDeletionFailureException e) {
      failed = e.numberFailedObjects();
      LOGGER.debug("Failed to delete {} files (no further details available)", failed, e);
    }
    return DeleteSummary.of(files.size() - failed, failed);
  }

  private DeleteSummary hadoopDeleteMultiple(Stream<String> filesAsStrings) {
    @SuppressWarnings("resource")
    FileIO fileIo = resolvingFileIO();

    return filesAsStrings
        .map(
            f -> {
              try {
                fileIo.deleteFile(f);
                return DeleteResult.SUCCESS;
              } catch (Exception e) {
                LOGGER.debug("Failed to delete {}", f, e);
                return DeleteResult.FAILURE;
              }
            })
        .reduce(DeleteSummary.EMPTY, DeleteSummary::add, DeleteSummary::add);
  }
}
