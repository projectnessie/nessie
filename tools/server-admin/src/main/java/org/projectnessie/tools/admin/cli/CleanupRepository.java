/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.tools.admin.cli;

import static java.time.Instant.ofEpochMilli;
import static java.time.temporal.ChronoUnit.MICROS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.projectnessie.versioned.storage.cleanup.Cleanup.createCleanup;
import static org.projectnessie.versioned.storage.cleanup.CleanupParams.DEFAULT_ALLOWED_FALSE_POSITIVE_PROBABILITY;
import static org.projectnessie.versioned.storage.cleanup.CleanupParams.DEFAULT_EXPECTED_OBJ_COUNT;
import static org.projectnessie.versioned.storage.cleanup.CleanupParams.DEFAULT_FALSE_POSITIVE_PROBABILITY;
import static org.projectnessie.versioned.storage.cleanup.CleanupParams.DEFAULT_RECENT_OBJ_IDS_FILTER_SIZE;
import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;

import java.time.Duration;
import org.projectnessie.versioned.storage.cleanup.CleanupParams;
import org.projectnessie.versioned.storage.cleanup.MustRestartWithBiggerFilterException;
import org.projectnessie.versioned.storage.cleanup.ResolveResult;
import picocli.CommandLine;

@CommandLine.Command(
    name = "cleanup-repository",
    mixinStandardHelpOptions = true,
    description = {
      "Cleanup unreferenced data from Nessie's repository.",
      "This is a two-phase implementation that first identifies the objects that are referenced and the second phase scans the whole repository and deletes objects that are unreferenced.",
      "It is recommended to run this command regularly, but with appropriate rate limits using the "
          + CleanupRepository.COMMIT_RATE
          + ", "
          + CleanupRepository.OBJ_RATE
          + ", "
          + CleanupRepository.SCAN_OBJ_RATE
          + ", "
          + CleanupRepository.PURGE_OBJ_RATE
          + " which does not overload your backend database system.",
      "The implementation uses a bloom-filter to identify the IDs of referenced objects. The default setting is to allow for "
          + DEFAULT_EXPECTED_OBJ_COUNT
          + " objects in the backend database with an FPP of "
          + DEFAULT_FALSE_POSITIVE_PROBABILITY
          + ". These values should serve most repositories. However, if your repository is quite big, you should supply a higher expected object count using the "
          + CleanupRepository.OBJ_COUNT
          + " option. If the implementation detected that the bloom-filter would exceed the maximum allowed FPP, it would restart with a higher number of expected objects.",
      "In rare situations with an extremely huge amount of objects, the data structures may require a lot of memory. The estimated heap pressure for the contextual data structures is printed to the console.",
      "If you are unsure whether this command works fine, specify the "
          + CleanupRepository.DRY_RUN
          + " option to perform all operations except deleting objects."
    })
public class CleanupRepository extends BaseCommand {

  public static final String OBJ_COUNT = "--obj-count";
  public static final String COMMIT_RATE = "--commit-rate";
  public static final String OBJ_RATE = "--obj-rate";
  public static final String SCAN_OBJ_RATE = "--scan-obj-rate";
  public static final String PURGE_OBJ_RATE = "--purge-obj-rate";
  public static final String DRY_RUN = "--dry-run";
  public static final String REFERENCED_GRACE = "--referenced-grace";

  @CommandLine.Option(
      names = {DRY_RUN},
      description = "Perform all operations, but do not delete any object .")
  private boolean dryRun;

  @CommandLine.Option(
      names = {REFERENCED_GRACE},
      description =
          "Grace-time for newly created objects to not be deleted. Default is just \"now\". Specified using the ISO-8601 format, for example P1D (24 hours) or PT2H (2 hours) or P10D12H (10 * 24 + 10 hours).")
  private Duration objReferencedGrace;

  @CommandLine.Option(
      names = {OBJ_COUNT},
      description = "Number of expected objects, defaults to " + DEFAULT_EXPECTED_OBJ_COUNT + '.')
  private long expectedObjCount = DEFAULT_EXPECTED_OBJ_COUNT;

  @CommandLine.Option(
      names = {"--fpp"},
      description =
          "Default false-positive-probability to detect referenced objects, defaults to "
              + DEFAULT_FALSE_POSITIVE_PROBABILITY
              + '.')
  private double falsePositiveProbability = DEFAULT_FALSE_POSITIVE_PROBABILITY;

  @CommandLine.Option(
      names = {"--allowed-fpp"},
      description =
          "Maximum allowed false-positive-probability to detect referenced objects, defaults to "
              + DEFAULT_ALLOWED_FALSE_POSITIVE_PROBABILITY
              + '.')
  private double allowedFalsePositiveProbability = DEFAULT_ALLOWED_FALSE_POSITIVE_PROBABILITY;

  @CommandLine.Option(
      names = {COMMIT_RATE},
      description =
          "Allowed number of commits to process during the 'resolve' phase per second. Default is unlimited.")
  private int resolveCommitRatePerSecond = 0;

  @CommandLine.Option(
      names = {OBJ_RATE},
      description =
          "Allowed number of objects to process during the 'resolve' phase per second. Default is unlimited.")
  private int resolveObjRatePerSecond = 0;

  @CommandLine.Option(
      names = {"--pending-objs-batch-size"},
      description = "")
  private int pendingObjsBatchSize = CleanupParams.DEFAULT_PENDING_OBJS_BATCH_SIZE;

  @CommandLine.Option(
      names = {"--allow-duplicate-commit-traversal"},
      description =
          "Allow traversal of the same commit more than once. This is disabled by default.")
  private boolean allowDuplicateCommitTraversals = false;

  @CommandLine.Option(
      names = {SCAN_OBJ_RATE},
      description =
          "Allowed number of objects to scan during the 'purge' phase per second. Default is unlimited.")
  private int purgeScanObjRatePerSecond = 0;

  @CommandLine.Option(
      names = {PURGE_OBJ_RATE},
      description =
          "Allowed number of objects to delete during the 'purge' phase per second. Default is unlimited.")
  private int purgeDeleteObjRatePerSecond = 0;

  @CommandLine.Option(
      names = {"--recent-objs-ids-filter-size"},
      description =
          "Size of the filter to recognize recently processed objects. This helps to reduce effort, but should be kept to a reasonable number. Defaults to "
              + DEFAULT_RECENT_OBJ_IDS_FILTER_SIZE
              + '.')
  private int recentObjIdsFilterSize = DEFAULT_RECENT_OBJ_IDS_FILTER_SIZE;

  @Override
  public Integer call() {
    warnOnInMemory();

    if (!repositoryLogic(persist).repositoryExists()) {
      spec.commandLine().getErr().println("Nessie repository does not exist");
      return EXIT_CODE_REPO_DOES_NOT_EXIST;
    }

    CleanupParams cleanupParams =
        CleanupParams.builder()
            .expectedObjCount(expectedObjCount)
            .falsePositiveProbability(falsePositiveProbability)
            .allowedFalsePositiveProbability(allowedFalsePositiveProbability)
            .resolveCommitRatePerSecond(resolveCommitRatePerSecond)
            .resolveObjRatePerSecond(resolveObjRatePerSecond)
            .pendingObjsBatchSize(pendingObjsBatchSize)
            .allowDuplicateCommitTraversals(allowDuplicateCommitTraversals)
            .purgeScanObjRatePerSecond(purgeScanObjRatePerSecond)
            .purgeDeleteObjRatePerSecond(purgeDeleteObjRatePerSecond)
            .recentObjIdsFilterSize(recentObjIdsFilterSize)
            .dryRun(dryRun)
            .build();

    // timestamp in MICROseconds since epoch
    var maxObjReferenced = persist.config().currentTimeMicros();
    if (objReferencedGrace != null) {
      var millis = objReferencedGrace.toMillis();
      if (millis < 0) {
        spec.commandLine()
            .getErr()
            .println("Argument for " + objReferencedGrace + " must not be negative!");
        return EXIT_CODE_GENERIC_ERROR;
      }
      maxObjReferenced -= MILLISECONDS.toMicros(millis);
    }

    ResolveResult resolveResult;
    while (true) {
      var cleanup = createCleanup(cleanupParams);
      var referencedObjectsContext =
          cleanup.buildReferencedObjectsContext(persist, maxObjReferenced);
      var referencedObjectsResolver =
          cleanup.createReferencedObjectsResolver(referencedObjectsContext);

      try {
        spec.commandLine()
            .getOut()
            .printf(
                "Identifying referenced objects, processing %s commits per second, processing %s objects per second, expecting max %d objects, estimated context heap pressure: %.3f M%n",
                resolveCommitRatePerSecond > 0
                    ? Integer.toString(resolveCommitRatePerSecond)
                    : "unlimited",
                resolveObjRatePerSecond > 0
                    ? Integer.toString(resolveObjRatePerSecond)
                    : "unlimited",
                expectedObjCount,
                (double) referencedObjectsResolver.estimatedHeapPressure() / 1024L / 1024L);
        resolveResult = referencedObjectsResolver.resolve();

        spec.commandLine()
            .getOut()
            .printf(
                "Finished identifying referenced objects after %s. Processed %d references, %d commits, %d objects, %d contents.%n",
                resolveResult.stats().duration(),
                resolveResult.stats().numReferences(),
                resolveResult.stats().numUniqueCommits(),
                resolveResult.stats().numObjs(),
                resolveResult.stats().numContents());

        var purgeObjects = cleanup.createPurgeObjects(resolveResult.purgeObjectsContext());

        spec.commandLine()
            .getOut()
            .printf(
                "%s unreferenced objects, referenced before %s, scanning %s objects per second, deleting %s objects per second, estimated context heap pressure: %.3f M%n",
                dryRun ? "Dry-run cleanup" : "Purging",
                ofEpochMilli(MICROSECONDS.toMillis(maxObjReferenced))
                    .plus(maxObjReferenced % 1000, MICROS),
                purgeScanObjRatePerSecond > 0
                    ? Integer.toString(purgeScanObjRatePerSecond)
                    : "unlimited",
                purgeDeleteObjRatePerSecond > 0
                    ? Integer.toString(purgeDeleteObjRatePerSecond)
                    : "unlimited",
                (double) purgeObjects.estimatedHeapPressure() / 1024L / 1024L);

        var purgeResult = purgeObjects.purge();

        spec.commandLine()
            .getOut()
            .printf(
                "Finished purging unreferenced objects after %s. Scanned %d objects, %d were deleted.%n",
                resolveResult.stats().duration(),
                purgeResult.stats().numScannedObjs(),
                purgeResult.stats().numPurgedObjs());

        break;
      } catch (MustRestartWithBiggerFilterException e) {
        var prev = cleanupParams.expectedObjCount();
        cleanupParams = cleanupParams.withIncreasedExpectedObjCount();

        spec.commandLine()
            .getErr()
            .printf(
                "Restarting identify referenced objects stage with increased expected object count from %d to %d. Please run this command the next time with the option '%s %d'%n",
                prev,
                cleanupParams.expectedObjCount(),
                OBJ_COUNT,
                cleanupParams.expectedObjCount());
      }
    }

    return 0;
  }
}
