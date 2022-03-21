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

import com.google.common.hash.BloomFilter;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.projectnessie.api.params.FetchOption;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.Detached;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

/**
 * Contains the methods to identify expired contents that execute in spark executor for {@link
 * GCImpl#identifyExpiredContents(SparkSession)}.
 */
public class IdentifyExpiredContentsPerExecutor extends IdentifyContentsPerExecutor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IdentifyExpiredContentsPerExecutor.class);

  public IdentifyExpiredContentsPerExecutor(GCParams gcParams) {
    super(gcParams);
  }

  protected SerializableFunction1<scala.collection.Iterator<String>, scala.collection.Iterator<Row>>
      getExpiredContentRowsFunc(
          BloomFilter<Content> liveContentsBloomFilter,
          String runId,
          Timestamp startedAt,
          Map<String, Instant> droppedRefTimeMap) {
    return result ->
        getExpiredContentRows(result, liveContentsBloomFilter, runId, startedAt, droppedRefTimeMap);
  }

  /**
   * convert scala Iterator of reference into scala Iterator of {@link Row}.
   *
   * <p>Each reference can produce one or more rows.
   */
  private scala.collection.Iterator<Row> getExpiredContentRows(
      scala.collection.Iterator<String> references,
      BloomFilter<Content> liveContentsBloomFilter,
      String runId,
      Timestamp startedAt,
      Map<String, Instant> droppedRefTimeMap) {
    NessieApiV1 api = GCUtil.getApi(gcParams.getNessieClientConfigs());
    TaskContext.get()
        .addTaskCompletionListener(
            context -> {
              LOGGER.info("Closing the nessie api for compute expired contents task");
              api.close();
            });
    return references.flatMap(
        reference -> {
          Reference ref = GCUtil.deserializeReference(reference);
          return JavaConverters.asScalaIterator(
                  walkAllCommitsInReference(
                      api,
                      ref,
                      liveContentsBloomFilter,
                      runId,
                      startedAt,
                      getCutoffTimeForRef(reference, droppedRefTimeMap)))
              .toTraversable();
        });
  }

  private Iterator<Row> walkAllCommitsInReference(
      NessieApiV1 api,
      Reference reference,
      BloomFilter<Content> liveContentsBloomFilter,
      String runId,
      Timestamp startedAt,
      Instant cutoffTime) {
    try {
      Iterator<ImmutableContentWithCommitInfo> iterator =
          api
              .getCommitLog()
              .hashOnRef(reference.getHash())
              .refName(Detached.REF_NAME)
              .fetch(FetchOption.ALL)
              .stream()
              .flatMap(
                  entry ->
                      entry.getOperations().stream()
                          .filter(Operation.Put.class::isInstance)
                          .map(Operation.Put.class::cast)
                          .map(Operation.Put::getContent)
                          .filter(
                              content ->
                                  ((content instanceof IcebergTable
                                          || content instanceof IcebergView)
                                      && getSnapshotId(content) != -1))
                          .map(
                              content ->
                                  ImmutableContentWithCommitInfo.builder()
                                      .content(content)
                                      .commitHash(entry.getCommitMeta().getHash())
                                      .commitTime(entry.getCommitMeta().getCommitTime())
                                      .build()))
              .iterator();

      return new Iterator<Row>() {
        // To filter only the commits that are expired based on cutoff time.
        // cutoff time also acts as a commit protection time for ongoing
        // or new commits created after step-1 of identify gc.
        final Predicate<Instant> cutoffTimePredicate =
            commitTime -> commitTime.compareTo(cutoffTime) < 0;

        // when no live bloom filter exist for this content id, all the contents are
        // definitely expired.
        // Checking against live contents bloom filter can result
        //    no  --> content is considered expired (filter cannot say 'no' for live
        // contents).
        //    may be  -->  content is considered live (filter can say 'maybe' for expired
        // contents).
        // Worst case few expired contents will be considered live due to bloom filter
        // fpp.
        // But live contents never be considered as expired.
        final Predicate<Content> expiredContentPredicate =
            content ->
                (liveContentsBloomFilter == null || !liveContentsBloomFilter.mightContain(content));

        @Override
        public boolean hasNext() {
          return iterator.hasNext();
        }

        @Override
        public Row next() {
          ContentWithCommitInfo contentWithCommitInfo = iterator.next();

          boolean isExpired =
              cutoffTimePredicate.test(contentWithCommitInfo.getCommitTime())
                  && expiredContentPredicate.test(contentWithCommitInfo.getContent());
          return fillRow(
              reference,
              contentWithCommitInfo.getContent(),
              runId,
              startedAt,
              contentWithCommitInfo.getCommitHash(),
              isExpired);
        }
      };
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static Row fillRow(
      Reference reference,
      Content content,
      String runId,
      Timestamp startedAt,
      String commitHash,
      boolean isExpired) {
    return IdentifiedResultsRepo.createContentRow(
        content,
        runId,
        startedAt,
        getSnapshotId(content),
        reference,
        commitHash,
        getMetadataFileLocation(content),
        isExpired);
  }

  private static long getSnapshotId(Content content) {
    long snapshotId;
    switch (content.getType()) {
      case ICEBERG_VIEW:
        snapshotId = ((IcebergView) content).getVersionId();
        break;
      case ICEBERG_TABLE:
      default:
        snapshotId = ((IcebergTable) content).getSnapshotId();
    }
    return snapshotId;
  }

  private static String getMetadataFileLocation(Content content) {
    String location;
    switch (content.getType()) {
      case ICEBERG_VIEW:
        location = ((IcebergView) content).getMetadataLocation();
        break;
      case ICEBERG_TABLE:
      default:
        location = ((IcebergTable) content).getMetadataLocation();
    }
    return location;
  }
}
