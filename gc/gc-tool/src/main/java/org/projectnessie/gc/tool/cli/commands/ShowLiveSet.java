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
package org.projectnessie.gc.tool.cli.commands;

import static java.lang.String.format;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.gc.contents.LiveContentSet;
import org.projectnessie.gc.contents.LiveContentSetsRepository;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.gc.tool.cli.options.EnvironmentDefaultProvider;
import org.projectnessie.storage.uri.StorageUri;
import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;

@CommandLine.Command(
    name = "show",
    mixinStandardHelpOptions = true,
    defaultValueProvider = EnvironmentDefaultProvider.class,
    description =
        "Show information of a live-content-set, "
            + "must not be used with the in-memory contents-storage.")
public class ShowLiveSet extends BaseLiveSetCommand {

  @CommandLine.Option(
      names = {"-D", "--with-deferred-deletes"},
      description = "Show deferred deletes.")
  private boolean showDeferredDeletes;

  @CommandLine.Option(
      names = {"-C", "--with-content-references"},
      description = "Show content references.")
  private boolean showContentReferences;

  @CommandLine.Option(
      names = {"-B", "--with-base-locations"},
      description = "Show base locations.")
  private boolean showBaseLocations;

  @Override
  protected Integer call(
      LiveContentSet liveContentSet, LiveContentSetsRepository liveContentSetsRepository) {
    out.println("Live content set information:");

    out.println(Ansi.AUTO.text("@|italic Timestamps shown in " + zoneId() + "|@"));

    out.printf(
        "%-18s %s%n"
            + "%-18s %s%n"
            + "%-18s %s%n"
            + "%-18s %s%n"
            + "%-18s %s%n"
            + "%-18s %s%n"
            + "%-18s %s%n",
        "ID",
        liveContentSet.id(),
        "Status",
        liveContentSet.status(),
        "Created",
        liveContentSet.created(),
        "Identify complete",
        instantAsString(liveContentSet.identifyCompleted()),
        "Expiry started",
        instantAsString(liveContentSet.expiryStarted()),
        "Expiry finished",
        instantAsString(liveContentSet.expiryCompleted()),
        "Error",
        asString(liveContentSet.errorMessage()));

    out.printf("%-18s %s%n", "Content count", liveContentSet.fetchDistinctContentIdCount());

    if (showContentReferences || showBaseLocations) {
      out.println();
      String title;
      if (!showContentReferences) {
        title = "List of base locations";
      } else if (!showBaseLocations) {
        title = "List of content references";
      } else {
        title = "List of base locations and content references:|@";
      }
      out.println(Ansi.AUTO.text("@|bold,underline " + title + ":|@"));

      String contentRefFormat = "    %-15s %-64s %-50s %-20s %s";
      try (Stream<String> contentids = liveContentSet.fetchContentIds()) {
        contentids.forEach(
            contentId -> {
              out.println(Ansi.AUTO.text("@|bold   Content ID " + contentId + "|@"));

              if (showBaseLocations) {
                try (Stream<StorageUri> baseLocations =
                    liveContentSet.fetchBaseLocations(contentId)) {
                  baseLocations.forEach(l -> out.printf("    Base location: %s%n", l));
                }
              }
              if (showContentReferences) {
                try (Stream<ContentReference> contentReferences =
                    liveContentSet.fetchContentReferences(contentId)) {
                  AtomicBoolean first = new AtomicBoolean(true);
                  contentReferences
                      .peek(
                          x -> {
                            if (first.compareAndSet(true, false)) {
                              out.println(
                                  Ansi.AUTO.text(
                                      format(
                                          "@|underline " + contentRefFormat + "|@",
                                          "Content Type",
                                          "Commit ID",
                                          "Content Key",
                                          "Snapshot ID",
                                          "Metadata Location")));
                            }
                          })
                      .forEach(
                          r ->
                              out.println(
                                  format(
                                      contentRefFormat,
                                      r.contentType(),
                                      r.commitId(),
                                      r.contentKey(),
                                      r.snapshotId(),
                                      r.metadataLocation())));
                }
              }
            });
      }
      out.println("End of listing.");
    }

    if (showDeferredDeletes) {
      out.println();
      out.println(Ansi.AUTO.text("@|bold,underline List of deferred deletes:|@"));
      String format = "%-35s %-60s %s";
      out.println(
          Ansi.AUTO.text(
              format("@|underline " + format + "|@", "Modified", "Base URI", "File path")));
      try (Stream<FileReference> deferredDeletions = liveContentSet.fetchFileDeletions()) {
        deferredDeletions.forEach(
            f ->
                out.println(
                    format(
                        format,
                        Instant.ofEpochMilli(f.modificationTimeMillisEpoch()),
                        f.base(),
                        f.path())));
      }
      out.println("End of listing.");
    }

    return 0;
  }
}
