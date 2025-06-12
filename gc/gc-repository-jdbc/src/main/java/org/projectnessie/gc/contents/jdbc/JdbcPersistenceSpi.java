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
package org.projectnessie.gc.contents.jdbc;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static org.projectnessie.gc.contents.ContentReference.icebergContent;
import static org.projectnessie.gc.contents.jdbc.JdbcHelper.isIntegrityConstraintViolation;
import static org.projectnessie.gc.contents.jdbc.SqlDmlDdl.ADD_CONTENT;
import static org.projectnessie.gc.contents.jdbc.SqlDmlDdl.DELETE_FILE_DELETIONS;
import static org.projectnessie.gc.contents.jdbc.SqlDmlDdl.DELETE_LIVE_CONTENTS;
import static org.projectnessie.gc.contents.jdbc.SqlDmlDdl.DELETE_LIVE_CONTENT_SET;
import static org.projectnessie.gc.contents.jdbc.SqlDmlDdl.DELETE_LIVE_SET_LOCATIONS;
import static org.projectnessie.gc.contents.jdbc.SqlDmlDdl.ERROR_LENGTH;
import static org.projectnessie.gc.contents.jdbc.SqlDmlDdl.FINISH_EXPIRE;
import static org.projectnessie.gc.contents.jdbc.SqlDmlDdl.FINISH_IDENTIFY;
import static org.projectnessie.gc.contents.jdbc.SqlDmlDdl.INSERT_CONTENT_LOCATION;
import static org.projectnessie.gc.contents.jdbc.SqlDmlDdl.INSERT_FILE_DELETIONS;
import static org.projectnessie.gc.contents.jdbc.SqlDmlDdl.SELECT_ALL_LIVE_CONTENT_SETS;
import static org.projectnessie.gc.contents.jdbc.SqlDmlDdl.SELECT_CONTENT_COUNT;
import static org.projectnessie.gc.contents.jdbc.SqlDmlDdl.SELECT_CONTENT_IDS;
import static org.projectnessie.gc.contents.jdbc.SqlDmlDdl.SELECT_CONTENT_LOCATION;
import static org.projectnessie.gc.contents.jdbc.SqlDmlDdl.SELECT_CONTENT_LOCATION_ALL;
import static org.projectnessie.gc.contents.jdbc.SqlDmlDdl.SELECT_CONTENT_REFERENCES;
import static org.projectnessie.gc.contents.jdbc.SqlDmlDdl.SELECT_FILE_DELETIONS;
import static org.projectnessie.gc.contents.jdbc.SqlDmlDdl.SELECT_LIVE_CONTENT_SET;
import static org.projectnessie.gc.contents.jdbc.SqlDmlDdl.START_EXPIRE;
import static org.projectnessie.gc.contents.jdbc.SqlDmlDdl.START_IDENTIFY;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Content.Type.ICEBERG_VIEW;

import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.MustBeClosed;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.immutables.value.Value;
import org.intellij.lang.annotations.Language;
import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.gc.contents.LiveContentSet;
import org.projectnessie.gc.contents.LiveContentSetNotFoundException;
import org.projectnessie.gc.contents.jdbc.JdbcHelper.FromRow;
import org.projectnessie.gc.contents.jdbc.JdbcHelper.Prepare;
import org.projectnessie.gc.contents.jdbc.JdbcHelper.ResultSetSplit;
import org.projectnessie.gc.contents.jdbc.JdbcHelper.WithStatement;
import org.projectnessie.gc.contents.spi.PersistenceSpi;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.types.ContentTypes;
import org.projectnessie.storage.uri.StorageUri;

@Value.Immutable
public abstract class JdbcPersistenceSpi implements PersistenceSpi {

  public static Builder builder() {
    return ImmutableJdbcPersistenceSpi.builder();
  }

  @SuppressWarnings({"UnusedReturnValue", "unused"})
  public interface Builder {
    Builder dataSource(DataSource dataSource);

    Builder fetchSize(int fetchSize);

    JdbcPersistenceSpi build();
  }

  @Value.Lazy
  protected String productName() {
    try (Connection conn = dataSource().getConnection()) {
      return conn.getMetaData().getDatabaseProductName().toLowerCase(Locale.ROOT);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void startIdentifyLiveContents(UUID liveSetId, Instant created) {
    singleStatement(
        START_IDENTIFY,
        (conn, stmt) -> {
          stmt.setString(1, liveSetId.toString());
          stmt.setTimestamp(2, Timestamp.from(created));
          stmt.setString(3, LiveContentSet.Status.IDENTIFY_IN_PROGRESS.name());
          try {
            stmt.executeUpdate();
          } catch (SQLException e) {
            if (isIntegrityConstraintViolation(e)) {
              throw new IllegalStateException("Duplicate liveSetId " + liveSetId);
            }
            throw e;
          }
          return null;
        },
        true);
  }

  @Override
  public void finishedIdentifyLiveContents(
      UUID liveSetId, Instant finished, @Nullable @jakarta.annotation.Nullable Throwable failure) {
    singleStatement(
        FINISH_IDENTIFY,
        (conn, stmt) -> {
          stmt.setTimestamp(1, Timestamp.from(finished));
          if (failure != null) {
            stmt.setString(2, LiveContentSet.Status.IDENTIFY_FAILED.name());
            stmt.setString(3, trimError(failure.toString()));
          } else {
            stmt.setString(2, LiveContentSet.Status.IDENTIFY_SUCCESS.name());
            stmt.setNull(3, Types.VARCHAR);
          }
          stmt.setString(4, liveSetId.toString());
          stmt.setString(5, LiveContentSet.Status.IDENTIFY_IN_PROGRESS.name());
          verifyStatusUpdate(conn, stmt, LiveContentSet.Status.IDENTIFY_IN_PROGRESS, liveSetId);
          return null;
        },
        true);
  }

  @Override
  public LiveContentSet startExpireContents(UUID liveSetId, Instant started) {
    return singleStatement(
        START_EXPIRE,
        (conn, stmt) -> {
          stmt.setTimestamp(1, Timestamp.from(started));
          stmt.setString(2, LiveContentSet.Status.EXPIRY_IN_PROGRESS.name());
          stmt.setString(3, liveSetId.toString());
          stmt.setString(4, LiveContentSet.Status.IDENTIFY_SUCCESS.name());
          verifyStatusUpdate(conn, stmt, LiveContentSet.Status.IDENTIFY_SUCCESS, liveSetId);
          return currentLiveSet(conn, liveSetId);
        },
        true);
  }

  @Override
  public LiveContentSet finishedExpireContents(
      UUID liveSetId, Instant finished, @Nullable @jakarta.annotation.Nullable Throwable failure) {
    return singleStatement(
        FINISH_EXPIRE,
        (conn, stmt) -> {
          stmt.setTimestamp(1, Timestamp.from(finished));
          if (failure != null) {
            stmt.setString(2, LiveContentSet.Status.EXPIRY_FAILED.name());
            stmt.setString(3, trimError(getStackTraceAsString(failure)));
          } else {
            stmt.setString(2, LiveContentSet.Status.EXPIRY_SUCCESS.name());
            stmt.setNull(3, Types.VARCHAR);
          }
          stmt.setString(4, liveSetId.toString());
          stmt.setString(5, LiveContentSet.Status.EXPIRY_IN_PROGRESS.name());
          verifyStatusUpdate(conn, stmt, LiveContentSet.Status.EXPIRY_IN_PROGRESS, liveSetId);
          return currentLiveSet(conn, liveSetId);
        },
        true);
  }

  static String trimError(String s) {
    if (s.length() <= ERROR_LENGTH) {
      return s;
    }
    return s.substring(0, ERROR_LENGTH - " ... (truncated)".length()) + " ... (truncated)";
  }

  private void verifyStatusUpdate(
      Connection conn, PreparedStatement stmt, LiveContentSet.Status expectedStatus, UUID liveSetId)
      throws SQLException {
    if (stmt.executeUpdate() != 1) {
      LiveContentSet current = currentLiveSet(conn, liveSetId);
      Preconditions.checkState(
          current.status() == expectedStatus,
          "Expected current status of " + expectedStatus + ", but is " + current.status());
    }
  }

  @Override
  public long fetchDistinctContentIdCount(UUID liveSetId) {
    return singleStatement(
        SELECT_CONTENT_COUNT,
        (conn, stmt) -> {
          stmt.setString(1, liveSetId.toString());
          try (ResultSet rs = stmt.executeQuery()) {
            rs.next();
            return rs.getLong(1);
          }
        },
        false);
  }

  @Override
  @MustBeClosed
  public Stream<String> fetchContentIds(UUID liveSetId) {
    return streamingResult(
        SELECT_CONTENT_IDS, stmt -> stmt.setString(1, liveSetId.toString()), rs -> rs.getString(1));
  }

  @Override
  public long addIdentifiedLiveContent(UUID liveSetId, Stream<ContentReference> contentReference) {
    return singleStatement(
        decorateInsertStatement(ADD_CONTENT),
        (conn, stmt) -> {
          stmt.setString(1, liveSetId.toString());
          long count = 0L;
          for (Iterator<ContentReference> iter = contentReference.iterator(); iter.hasNext(); ) {
            ContentReference ref = iter.next();
            stmt.setString(2, ref.contentId());
            stmt.setString(3, ref.commitId());
            stmt.setString(4, ref.contentKey().toPathString());
            stmt.setString(5, ref.contentType().name());
            if (ref.contentType().equals(ICEBERG_TABLE) || ref.contentType().equals(ICEBERG_VIEW)) {
              stmt.setString(
                  6,
                  Objects.requireNonNull(
                      ref.metadataLocation(),
                      "Illegal null metadataLocation in ContentReference for ICEBERG_TABLE/ICEBERG_VIEW"));
              stmt.setLong(
                  7,
                  Objects.requireNonNull(
                      ref.snapshotId(),
                      "Illegal null snapshotId in ContentReference for ICEBERG_TABLE/ICEBERG_VIEW"));
            } else {
              throw new UnsupportedOperationException(
                  "Unsupported content type " + ref.contentType());
            }
            // TODO add batch updates
            if (stmt.executeUpdate() != 0) {
              count++;
            }
          }
          return count;
        },
        true);
  }

  @Override
  @MustBeClosed
  public Stream<ContentReference> fetchContentReferences(UUID liveSetId, String contentId) {
    return streamingResult(
        SELECT_CONTENT_REFERENCES,
        stmt -> {
          stmt.setString(1, liveSetId.toString());
          stmt.setString(2, contentId);
        },
        JdbcPersistenceSpi::contentReference);
  }

  @Override
  public void associateBaseLocations(
      UUID liveSetId, String contentId, Collection<StorageUri> baseLocations) {
    singleStatement(
        decorateInsertStatement(INSERT_CONTENT_LOCATION),
        (conn, stmt) -> {
          stmt.setString(1, liveSetId.toString());
          stmt.setString(2, contentId);
          for (StorageUri baseLocation : baseLocations) {
            stmt.setString(3, baseLocation.toString());
            stmt.executeUpdate();
          }
          return null;
        },
        true);
  }

  @Override
  @MustBeClosed
  public Stream<StorageUri> fetchBaseLocations(UUID liveSetId, String contentId) {
    return streamingResult(
        SELECT_CONTENT_LOCATION,
        stmt -> {
          stmt.setString(1, liveSetId.toString());
          stmt.setString(2, contentId);
        },
        rs -> StorageUri.of(rs.getString(1)));
  }

  @Override
  @MustBeClosed
  public Stream<StorageUri> fetchAllBaseLocations(UUID liveSetId) {
    return streamingResult(
        SELECT_CONTENT_LOCATION_ALL,
        stmt -> stmt.setString(1, liveSetId.toString()),
        rs -> StorageUri.of(rs.getString(1)));
  }

  @Override
  public void deleteLiveContentSet(UUID liveSetId) {
    singleStatement(
        DELETE_FILE_DELETIONS,
        (conn, stmt) -> {
          stmt.setString(1, liveSetId.toString());
          stmt.executeUpdate();
          try (PreparedStatement stmt2 = conn.prepareStatement(DELETE_LIVE_SET_LOCATIONS)) {
            stmt2.setString(1, liveSetId.toString());
            stmt2.executeUpdate();
          }
          try (PreparedStatement stmt2 = conn.prepareStatement(DELETE_LIVE_CONTENTS)) {
            stmt2.setString(1, liveSetId.toString());
            stmt2.executeUpdate();
          }
          try (PreparedStatement stmt2 = conn.prepareStatement(DELETE_LIVE_CONTENT_SET)) {
            stmt2.setString(1, liveSetId.toString());
            int cnt = stmt2.executeUpdate();
            Preconditions.checkState(cnt == 1, "Live content set not found %s", liveSetId);
          }
          return null;
        },
        true);
  }

  private LiveContentSet currentLiveSet(Connection conn, UUID liveSetId) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(SELECT_LIVE_CONTENT_SET)) {
      return queryLiveContentSet(liveSetId, stmt);
    }
  }

  @Override
  public LiveContentSet getLiveContentSet(UUID liveSetId) throws LiveContentSetNotFoundException {
    try {
      return singleStatement(
          SELECT_LIVE_CONTENT_SET, (conn, stmt) -> queryLiveContentSet(liveSetId, stmt), false);
    } catch (IllegalStateException e) {
      if (e.getCause() instanceof LiveContentSetNotFoundException) {
        throw (LiveContentSetNotFoundException) e.getCause();
      }
      throw e;
    }
  }

  private LiveContentSet queryLiveContentSet(UUID liveSetId, PreparedStatement stmt)
      throws SQLException {
    stmt.setString(1, liveSetId.toString());
    stmt.setFetchSize(fetchSize());
    try (ResultSet rs = stmt.executeQuery()) {
      rs.setFetchSize(fetchSize());
      if (!rs.next()) {
        throw new IllegalStateException(new LiveContentSetNotFoundException(liveSetId));
      }
      return liveContentSet(rs);
    }
  }

  @Override
  @MustBeClosed
  public Stream<LiveContentSet> getAllLiveContents() {
    return streamingResult(SELECT_ALL_LIVE_CONTENT_SETS, stmt -> {}, this::liveContentSet);
  }

  @Override
  public long addFileDeletions(UUID liveSetId, Stream<FileReference> files) {
    return singleStatement(
        decorateInsertStatement(INSERT_FILE_DELETIONS),
        (conn, stmt) -> {
          long count = 0L;
          for (Iterator<FileReference> iter = files.iterator(); iter.hasNext(); ) {
            FileReference f = iter.next();
            stmt.setString(1, liveSetId.toString());
            stmt.setString(2, f.base().toString());
            stmt.setString(3, f.path().toString());
            stmt.setLong(4, f.modificationTimeMillisEpoch());
            if (stmt.executeUpdate() != 0) {
              count++;
            }
          }
          return count;
        },
        true);
  }

  @Override
  public Stream<FileReference> fetchFileDeletions(UUID liveSetId) {
    return streamingResult(
        SELECT_FILE_DELETIONS,
        stmt -> stmt.setString(1, liveSetId.toString()),
        JdbcPersistenceSpi::fileObject);
  }

  static FileReference fileObject(ResultSet rs) throws SQLException {
    return FileReference.of(
        StorageUri.of(rs.getString(2)), StorageUri.of(rs.getString(1)), rs.getLong(3));
  }

  static ContentReference contentReference(ResultSet rs) throws SQLException {
    String contentId = rs.getString(1);
    String commitId = rs.getString(2);
    ContentKey contentKey = ContentKey.fromPathString(rs.getString(3));
    Content.Type contentType = ContentTypes.forName(rs.getString(4));
    if (contentType.equals(ICEBERG_TABLE) || contentType.equals(ICEBERG_VIEW)) {
      String metadataLocation = rs.getString(5);
      long snapshotId = rs.getLong(6);
      return icebergContent(
          contentType, contentId, commitId, contentKey, metadataLocation, snapshotId);
    } else {
      throw new IllegalStateException(
          "Unsupported content type '" + contentType + "' in repository");
    }
  }

  LiveContentSet liveContentSet(ResultSet rs) throws SQLException {
    Function<Timestamp, Instant> toInstant = t -> t != null ? t.toInstant() : null;
    return LiveContentSet.builder()
        .id(UUID.fromString(rs.getString(1)))
        .status(LiveContentSet.Status.valueOf(rs.getString(2)))
        .created(toInstant.apply(rs.getTimestamp(3)))
        .identifyCompleted(toInstant.apply(rs.getTimestamp(4)))
        .expiryStarted(toInstant.apply(rs.getTimestamp(5)))
        .expiryCompleted(toInstant.apply(rs.getTimestamp(6)))
        .errorMessage(rs.getString(7))
        .persistenceSpi(this)
        .build();
  }

  protected String decorateInsertStatement(String statement) {
    switch (productName()) {
      case "postgresql":
      case "h2":
        return statement + " ON CONFLICT DO NOTHING";
      case "mysql":
      case "mariadb":
        return statement.replace("INSERT", "INSERT IGNORE");
      default:
        throw new IllegalStateException("Unsupported database: " + productName());
    }
  }

  private Connection connection() {
    try {
      return dataSource().getConnection();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("SqlSourceToSinkFlow")
  <R> R singleStatement(
      @Language("SQL") String sql, WithStatement<R> withStatement, boolean modifyingStatement) {
    try (Connection conn = connection()) {
      boolean failed = true;
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        R r = withStatement.withStatement(conn, stmt);
        if (modifyingStatement) {
          conn.commit();
        }
        failed = false;
        return r;
      } finally {
        if (modifyingStatement && failed) {
          conn.rollback();
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  <R> Stream<R> streamingResult(@Language("SQL") String sql, Prepare prepare, FromRow<R> fromRow) {
    List<AutoCloseable> closeables = new ArrayList<>();

    ResultSetSplit<R> split =
        new ResultSetSplit<>(this::connection, fetchSize(), closeables::add, sql, prepare, fromRow);

    return StreamSupport.stream(split, false)
        .onClose(
            () -> {
              Exception ex = JdbcHelper.forClose(closeables);
              if (ex instanceof RuntimeException) {
                throw (RuntimeException) ex;
              }
              if (ex != null) {
                throw new RuntimeException(ex);
              }
            });
  }

  abstract DataSource dataSource();

  abstract int fetchSize();
}
