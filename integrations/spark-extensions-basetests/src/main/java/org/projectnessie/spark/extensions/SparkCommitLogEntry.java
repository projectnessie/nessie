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
package org.projectnessie.spark.extensions;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.model.CommitMeta;

/** Somewhat simplified Nessie commit log entry used by Spark-SQL extensions tests. */
@Value.Immutable
abstract class SparkCommitLogEntry {

  @Value.Default
  String getAuthor() {
    return "";
  }

  @Value.Default
  String getCommitter() {
    return "";
  }

  @Nullable
  abstract String getHash();

  @Nullable
  abstract String getMessage();

  @Value.Default
  String getSignedOffBy() {
    return "";
  }

  @Nullable
  abstract Timestamp getAuthorTime();

  @Nullable
  abstract Timestamp getCommitTime();

  abstract Map<String, String> getProperties();

  @SuppressWarnings("unchecked")
  static SparkCommitLogEntry fromShowLog(Object[] row) {
    return ImmutableSparkCommitLogEntry.builder()
        .author((String) row[0])
        // omit the committer! .committer((String) row[1])
        .hash((String) row[2])
        .message((String) row[3])
        .signedOffBy((String) row[4])
        .authorTime((Timestamp) row[5])
        // commit-time omitted
        .properties((Map<String, String>) row[7])
        .build();
  }

  static SparkCommitLogEntry fromCommitMeta(CommitMeta cm, String hash) {
    return ImmutableSparkCommitLogEntry.builder()
        .author(cm.getAuthor())
        .hash(hash)
        .message(cm.getMessage())
        .authorTime(cm.getAuthorTime() == null ? null : Timestamp.from(cm.getAuthorTime()))
        .commitTime(cm.getCommitTime() == null ? null : Timestamp.from(cm.getCommitTime()))
        .properties(cm.getProperties())
        .build();
  }

  SparkCommitLogEntry relevantFromMerge() {
    return ImmutableSparkCommitLogEntry.builder()
        .hash(getHash())
        .putAllProperties(withoutInternalProperties(getProperties()))
        .build();
  }

  SparkCommitLogEntry mergedCommits(SparkCommitLogEntry other) {
    Map<String, String> mergedCommitProperties = new HashMap<>();
    mergedCommitProperties.putAll(this.getProperties());
    mergedCommitProperties.putAll(other.getProperties());
    return ImmutableSparkCommitLogEntry.builder()
        .from(other)
        .message(
            other.getMessage()
                + "\n---------------------------------------------\n"
                + this.getMessage())
        .properties(mergedCommitProperties)
        .build();
  }

  private static Map<String, String> withoutInternalProperties(Map<String, String> properties) {
    return properties.entrySet().stream()
        .filter(e -> !e.getKey().startsWith("_"))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  SparkCommitLogEntry withoutHashAndTime() {
    return ImmutableSparkCommitLogEntry.builder()
        .from(this)
        .hash(null)
        .commitTime(null)
        .authorTime(null)
        .build();
  }
}
