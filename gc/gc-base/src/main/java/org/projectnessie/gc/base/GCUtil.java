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
package org.projectnessie.gc.base;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.spark.sql.SparkSession;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Reference;
import scala.Tuple2;

public final class GCUtil {

  private GCUtil() {}

  private static final ObjectMapper objectMapper = new ObjectMapper();

  /** Serialize {@link Reference} object using JSON Serialization. */
  public static String serializeReference(Reference reference) {
    try {
      return objectMapper.writeValueAsString(reference);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /** Deserialize JSON String to {@link Reference} object. */
  public static Reference deserializeReference(String reference) {
    try {
      return objectMapper.readValue(reference, Reference.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static Instant getInstantFromMicros(Long microsSinceEpoch) {
    return Instant.ofEpochSecond(
        TimeUnit.MICROSECONDS.toSeconds(microsSinceEpoch),
        TimeUnit.MICROSECONDS.toNanos(
            Math.floorMod(microsSinceEpoch, TimeUnit.SECONDS.toMicros(1))));
  }

  public static void getOrCreateEmptyBranch(NessieApiV1 api, String branchName) {
    try {
      api.getReference().refName(branchName).get();
    } catch (NessieNotFoundException e) {
      // create a gc branch pointing to NO_ANCESTOR hash.
      try {
        api.createReference().reference(Branch.of(branchName, null)).create();
      } catch (NessieNotFoundException | NessieConflictException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  /**
   * Loads the already existing nessie catalog of name {@code catalogName} and initialize to use the
   * {@code refName}.
   */
  public static NessieCatalog loadNessieCatalog(
      SparkSession sparkSession, String catalogName, String refName) {
    return (NessieCatalog)
        CatalogUtil.loadCatalog(
            NessieCatalog.class.getName(),
            catalogName,
            catalogConfWithRef(sparkSession, catalogName, refName),
            sparkSession.sparkContext().hadoopConfiguration());
  }

  /**
   * Builds the client builder; default({@link HttpClientBuilder}) or custom, based on the
   * configuration provided.
   *
   * @param configuration map of client builder configurations.
   * @return {@link NessieApiV1} object.
   */
  public static NessieApiV1 getApi(Map<String, String> configuration) {
    String clientBuilderClassName =
        configuration.get(NessieConfigConstants.CONF_NESSIE_CLIENT_BUILDER_IMPL);
    NessieClientBuilder builder;
    if (clientBuilderClassName == null) {
      // Use the default HttpClientBuilder
      builder = HttpClientBuilder.builder();
    } else {
      // Use the custom client builder
      try {
        builder =
            (NessieClientBuilder)
                Class.forName(clientBuilderClassName).getDeclaredConstructor().newInstance();
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException(
            String.format(
                "No custom client builder class found for '%s' ", clientBuilderClassName));
      } catch (InvocationTargetException
          | InstantiationException
          | IllegalAccessException
          | NoSuchMethodException e) {
        throw new IllegalArgumentException(
            String.format("Could not initialize '%s': ", clientBuilderClassName), e);
      }
    }
    return (NessieApiV1) builder.fromConfig(configuration::get).build(NessieApiV1.class);
  }

  /**
   * Traverse the live commits stream till an entry is seen for each live content key and reached
   * expired commits.
   *
   * @param foundAllLiveCommitHeadsBeforeCutoffTime condition to stop traversing
   * @param commits stream of {@link LogResponse.LogEntry}
   * @param commitHandler consumer of {@link LogResponse.LogEntry}
   */
  static void traverseLiveCommits(
      MutableBoolean foundAllLiveCommitHeadsBeforeCutoffTime,
      Stream<LogResponse.LogEntry> commits,
      Consumer<LogResponse.LogEntry> commitHandler) {
    Spliterator<LogResponse.LogEntry> src = commits.spliterator();
    // Use a Spliterator to limit the processed commits to the "live" commits - i.e. stop traversing
    // the expired commits once an entry is seen for each live content key.
    new Spliterators.AbstractSpliterator<LogResponse.LogEntry>(src.estimateSize(), 0) {
      private boolean more = true;

      @Override
      public boolean tryAdvance(Consumer<? super LogResponse.LogEntry> action) {
        if (!more) {
          return false;
        }
        more =
            src.tryAdvance(
                logEntry -> {
                  if (foundAllLiveCommitHeadsBeforeCutoffTime.isTrue()) {
                    // can stop traversing as found all the live commit heads
                    // for each live keys before cutoff time.
                    more = false;
                  } else {
                    // process this commit entry.
                    action.accept(logEntry);
                  }
                });
        return more;
      }
    }.forEachRemaining(commitHandler);
  }

  private static Map<String, String> catalogConfWithRef(
      SparkSession spark, String catalog, String branch) {
    Stream<Tuple2<String, String>> conf =
        Arrays.stream(
            spark
                .sparkContext()
                .conf()
                .getAllWithPrefix(String.format("spark.sql.catalog.%s.", catalog)));
    return conf.map(t -> t._1.equals("ref") ? Tuple2.apply(t._1, branch) : t)
        .collect(Collectors.toMap(t -> t._1, t -> t._2));
  }
}
