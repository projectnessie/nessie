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
package org.projectnessie.gc.base;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.projectnessie.api.params.FetchOption;
import org.projectnessie.client.StreamingUtil;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.RefLogResponse;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferenceMetadata;
import org.projectnessie.model.Tag;

/**
 * Encapsulates the logic to retrieve all (both live and expired) content including their content
 * keys over all commits in all named-references.
 */
public class GCImpl {
  private final GCParams gcParams;

  /**
   * Instantiates a new Gc.
   *
   * @param configuration the configuration
   * @param defaultCutOffTimeStamp the default cut off time stamp
   * @param deadReferenceCutOffTimeStamp optional cut off time for dead reference
   * @param cutOffTimestampPerRef the cutoff timestamp per ref
   */
  public GCImpl(
      Map<String, String> configuration,
      Instant defaultCutOffTimeStamp,
      Instant deadReferenceCutOffTimeStamp,
      Map<String, Instant> cutOffTimestampPerRef) {
    Objects.requireNonNull(configuration);
    Objects.requireNonNull(defaultCutOffTimeStamp);
    ImmutableGCParams.Builder builder =
        ImmutableGCParams.builder()
            .defaultCutOffTimestamp(defaultCutOffTimeStamp)
            .deadReferenceCutOffTimeStamp(deadReferenceCutOffTimeStamp)
            .cutOffTimestampPerRef(cutOffTimestampPerRef);
    withConfig(configuration, builder);
    gcParams = builder.build();
  }

  /**
   * Identify the expired contents using a two-step traversal algorithm.
   *
   * <h2>Algorithm for identifying the live contents and return the bloom filter per content-id</h2>
   *
   * <p>Walk through each reference(both live and dead) distributively (one spark task for each
   * reference).
   *
   * <p>While traversing from the head commit in a reference(use DETACHED reference to fetch commits
   * from dead reference), for each live commit (commit that is not expired based on cutoff time)
   * add the contents of put operation to bloom filter.
   *
   * <p>Collect the live content keys for this reference just before cutoff time (at first expired
   * commit head). Which is used to identify the commit head for each live content key at the time
   * of cutoff time to support the time travel.
   *
   * <p>While traversing the expired commits (commit that is expired based on cutoff time), if it is
   * a head commit content for its key, add it to bloom filter. Else move to next expired commit.
   *
   * <p>Stop traversing the expired commits if each live content key has processed one live commit
   * for it. This is an optimization to avoid traversing all the commits.
   *
   * <p>Collect bloom filter per content id from each task and merge them.
   *
   * <h2>Algorithm for identifying the expired contents and return the list of globally expired
   * contents per content id per reference </h2>
   *
   * <p>Walk through each reference(both live and dead) distributively (one spark task for each
   * reference).
   *
   * <p>For each commit in the reference (use DETACHED reference to fetch commits from dead
   * reference) check it against bloom filter to decide whether its contents in put operation are
   * globally expired or not. If globally expired, Add the contents to the expired output for this
   * content id for this reference.
   *
   * <p>Overall the contents after or equal to cutoff time and the contents that are mapped to
   * commit head of live keys at the time of cutoff timestamp will be retained.
   *
   * @param session spark session for distributed computation
   * @return {@link IdentifiedResult} object having expired contents per content id.
   */
  public IdentifiedResult identifyExpiredContents(SparkSession session) {
    try (NessieApiV1 api = GCUtil.getApi(gcParams.getNessieClientConfigs())) {
      DistributedIdentifyContents distributedIdentifyContents =
          new DistributedIdentifyContents(session, gcParams);
      // key is refName:hash and value is its dropped/assigned time.
      Map<String, Instant> droppedRefTimeMap = new HashMap<>();
      List<Reference> allRefs = getAllReferences(api, droppedRefTimeMap);
      long totalCommitsInDefaultReference = getTotalCommitsInDefaultReference(api);
      // Identify the live contents and return the bloom filter per content-id
      Map<String, ContentBloomFilter> liveContentsBloomFilterMap =
          distributedIdentifyContents.getLiveContentsBloomFilters(
              allRefs, totalCommitsInDefaultReference, droppedRefTimeMap);
      // Identify the expired contents
      return distributedIdentifyContents.getIdentifiedResults(liveContentsBloomFilterMap, allRefs);
    }
  }

  private List<Reference> getAllReferences(
      NessieApiV1 api, Map<String, Instant> droppedRefTimeMap) {
    List<Reference> references = api.getAllReferences().get().getReferences();
    List<Reference> deadReferences = collectDeadReferences(api, droppedRefTimeMap);
    List<Reference> allRefs = new ArrayList<>(references);
    if (deadReferences.size() > 0) {
      allRefs.addAll(deadReferences);
    }
    return allRefs;
  }

  private long getTotalCommitsInDefaultReference(NessieApiV1 api) {
    ReferenceMetadata defaultRefMetadata = null;
    try {
      defaultRefMetadata =
          api.getReference()
              .refName(api.getDefaultBranch().getName())
              .fetch(FetchOption.ALL)
              .get()
              .getMetadata();
    } catch (NessieNotFoundException ex) {
      // Ignore the failure and use default value.
    }
    return (defaultRefMetadata != null && defaultRefMetadata.getNumTotalCommits() != null)
        ? defaultRefMetadata.getNumTotalCommits()
        : 0;
  }

  private static void withConfig(
      Map<String, String> configuration, ImmutableGCParams.Builder builder) {
    String taskCountConf = configuration.get(GCConfigConstants.CONF_NESSIE_GC_SPARK_TASK_COUNT);
    if (taskCountConf != null) {
      int taskCount = Integer.parseInt(taskCountConf);
      if (taskCount <= 0) {
        throwException(taskCountConf, GCConfigConstants.CONF_NESSIE_GC_SPARK_TASK_COUNT);
      }
      builder.taskCount(taskCount);
    }

    String commitProtectionTimeConf =
        configuration.get(GCConfigConstants.CONF_NESSIE_GC_COMMIT_PROTECTION_TIME_IN_HOURS);
    if (commitProtectionTimeConf != null) {
      int commitProtectionTime = Integer.parseInt(commitProtectionTimeConf);
      if (commitProtectionTime < 0) {
        throwException(
            commitProtectionTimeConf,
            GCConfigConstants.CONF_NESSIE_GC_COMMIT_PROTECTION_TIME_IN_HOURS);
      }
      builder.commitProtectionTime(commitProtectionTime);
    }

    String expectedEntriesConf =
        configuration.get(GCConfigConstants.CONF_NESSIE_GC_BLOOM_FILTER_SIZE);
    if (expectedEntriesConf != null) {
      long expectedEntries = Long.parseLong(expectedEntriesConf);
      if (expectedEntries < 0) {
        throwException(expectedEntriesConf, GCConfigConstants.CONF_NESSIE_GC_BLOOM_FILTER_SIZE);
      }
      builder.bloomFilterExpectedEntries(expectedEntries);
    }

    String fppConf = configuration.get(GCConfigConstants.CONF_NESSIE_GC_BLOOM_FILTER_FPP);
    if (fppConf != null) {
      double fpp = Double.parseDouble(fppConf);
      if (!(fpp > 0.0D && fpp < 1.0D)) {
        throwException(fppConf, GCConfigConstants.CONF_NESSIE_GC_BLOOM_FILTER_FPP);
      }
      builder.bloomFilterFpp(fpp);
    }
    builder.nessieClientConfigs(configuration);
  }

  private static void throwException(String conf, String value) {
    throw new RuntimeException("Invalid configuration value " + value + " for " + conf);
  }

  private static List<Reference> collectDeadReferences(
      NessieApiV1 api, Map<String, Instant> droppedRefTimeMap) {
    Stream<RefLogResponse.RefLogResponseEntry> reflogStream;
    try {
      reflogStream =
          StreamingUtil.getReflogStream(
              api,
              null,
              null,
              String.format(
                  "reflog.operation == '%s' || reflog.operation == " + "'%s'",
                  RefLogResponse.RefLogResponseEntry.DELETE_REFERENCE,
                  RefLogResponse.RefLogResponseEntry.ASSIGN_REFERENCE),
              OptionalInt.empty());
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
    }
    return reflogStream
        .map(
            entry -> {
              String hash;
              switch (entry.getOperation()) {
                case RefLogResponse.RefLogResponseEntry.DELETE_REFERENCE:
                  hash = entry.getCommitHash();
                  break;
                case RefLogResponse.RefLogResponseEntry.ASSIGN_REFERENCE:
                  hash = entry.getSourceHashes().get(0);
                  break;
                default:
                  throw new RuntimeException(
                      entry.getOperation() + " operation found in dead reflog query");
              }
              droppedRefTimeMap.put(
                  entry.getRefName() + ":" + hash, getInstantFromMicros(entry.getOperationTime()));
              switch (entry.getRefType()) {
                case RefLogResponse.RefLogResponseEntry.BRANCH:
                  return Branch.of(entry.getRefName(), hash);
                case RefLogResponse.RefLogResponseEntry.TAG:
                  return Tag.of(entry.getRefName(), hash);
                default:
                  throw new RuntimeException(
                      entry.getRefType() + " type reference is found in dead reflog query");
              }
            })
        .collect(Collectors.toList());
  }

  private static Instant getInstantFromMicros(Long microsSinceEpoch) {
    return Instant.ofEpochSecond(
        TimeUnit.MICROSECONDS.toSeconds(microsSinceEpoch),
        TimeUnit.MICROSECONDS.toNanos(
            Math.floorMod(microsSinceEpoch, TimeUnit.SECONDS.toMicros(1))));
  }
}
