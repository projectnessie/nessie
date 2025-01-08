/*
 * Copyright (C) 2025 Dremio
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
package org.projectnessie.gc.tool.cli.options;

import picocli.CommandLine;
import java.util.ArrayList;
import java.util.List;

public class CleanGCTablesOptions {

  @CommandLine.Option(
    names = "--truncate-all",
    negatable = true,
    description =
      "Truncate all Nessie-Gc tables. " + "This option is incompatible with --inmemory.")
  boolean truncateAll;

  @CommandLine.Option(
    names = "--truncate-gc_file_deletion",
    negatable = true,
    description = "Truncate only Nessie-Gc gc-file-deletions table. ")
  boolean truncateGcFileDeletion;

  @CommandLine.Option(
    names = "--truncate-gc_live_set_content_locations",
    negatable = true,
    description = "Truncate only Nessie-Gc gc_live_set_content_locations table. ")
  boolean truncateGcLiveSetContentLocations;

  @CommandLine.Option(
    names = "--truncate-gc_live_set_contents",
    negatable = true,
    description = "Truncate only Nessie-Gc gc_live_set_contents table. ")
  boolean truncateGcLiveSetContents;

  @CommandLine.Option(
    names = "--truncate-gc_live_sets",
    negatable = true,
    description = "Truncate only Nessie-Gc gc_live_sets table. ")
  boolean truncateGcLiveSets;


  public List<String> getTruncateTableNames(){
    List<String> tableNames = new ArrayList<>();
    if(truncateAll){
      tableNames.addAll(List.of("gc_file_deletions","gc_live_set_contents",
        "gc_live_set_content_locations","gc_live_sets"));
    }else {
      if (truncateGcFileDeletion) {
        tableNames.add("gc_file_deletion");
      }
      if (truncateGcLiveSetContents) {
        tableNames.add("gc_live_set_contents");
      }
      if (truncateGcLiveSetContentLocations) {
        tableNames.add("gc_live_set_content_locations");
      }
      if (truncateGcLiveSets) {
        tableNames.add("gc_live_sets");
      }
    }
    return tableNames;
  }

}
