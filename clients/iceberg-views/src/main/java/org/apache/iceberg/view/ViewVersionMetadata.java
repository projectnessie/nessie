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
package org.apache.iceberg.view;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.ValidationException;

/** Metadata for versioning a view. */
public class ViewVersionMetadata {
  static final int VIEW_FORMAT_VERSION = 1;

  public static ViewVersionMetadata newViewVersionMetadata(
      BaseVersion version,
      String location,
      ViewDefinition definition,
      Map<String, String> properties) {

    return new ViewVersionMetadata(
        version, location, definition, properties, ImmutableList.of(), ImmutableList.of());
  }

  public static ViewVersionMetadata newViewVersionMetadata(
      BaseVersion version,
      String location,
      ViewDefinition definition,
      ViewVersionMetadata viewVersionMetadata,
      Map<String, String> properties) {
    return new ViewVersionMetadata(
        version,
        location,
        definition,
        properties,
        viewVersionMetadata.versions(),
        viewVersionMetadata.history());
  }

  // stored metadata
  private final String location;
  private final Map<String, String> properties;
  private final int currentVersionId;
  private List<Version> versions;
  private final Map<Integer, Version> versionsById;
  private List<HistoryEntry> versionLog;
  private final ViewDefinition definition;

  // Creates a new view version metadata by simply assigning variables
  public ViewVersionMetadata(
      String location,
      ViewDefinition definition,
      Map<String, String> properties,
      int currentVersionId,
      List<Version> versions,
      List<HistoryEntry> versionLog) {
    this.location = location;
    this.definition = definition;
    this.properties = properties;
    Preconditions.checkState(versions.size() > 0);
    Preconditions.checkState(versionLog.size() > 0);
    this.currentVersionId = currentVersionId;
    this.versions = versions;
    this.versionLog = versionLog;
    this.versionsById = indexVersions(versions);

    HistoryEntry last = null;
    for (HistoryEntry logEntry : versionLog) {
      if (last != null) {
        Preconditions.checkArgument(
            (logEntry.timestampMillis() - last.timestampMillis()) >= 0,
            "[BUG] Expected sorted version log entries.");
      }
      last = logEntry;
    }
  }

  // Creates a new view version metadata by adding the new version to log and history
  ViewVersionMetadata(
      BaseVersion version,
      String location,
      ViewDefinition definition,
      Map<String, String> properties,
      List<Version> versions,
      List<HistoryEntry> versionLog) {
    this.location = location;
    this.definition = definition;
    this.properties = properties;
    this.currentVersionId = version.versionId();

    List<Version> allVersions = new ArrayList<>(versions);
    allVersions.add(version);
    List<HistoryEntry> allHistory = new ArrayList<>(versionLog);
    allHistory.add(new VersionLogEntry(version.timestampMillis(), currentVersionId));

    int numVersionsToKeep =
        propertyAsInt(
            ViewProperties.VERSION_HISTORY_SIZE, ViewProperties.VERSION_HISTORY_SIZE_DEFAULT);
    if (allVersions.size() > numVersionsToKeep) {
      this.versions =
          ImmutableList.<Version>builder()
              .addAll(
                  allVersions.subList(allVersions.size() - numVersionsToKeep, allVersions.size()))
              .build();
      this.versionLog =
          ImmutableList.<HistoryEntry>builder()
              .addAll(allHistory.subList(allHistory.size() - numVersionsToKeep, allHistory.size()))
              .build();
    } else {
      this.versions = ImmutableList.<Version>builder().addAll(allVersions).build();
      this.versionLog = ImmutableList.<HistoryEntry>builder().addAll(allHistory).build();
    }

    this.versionsById = indexVersions(this.versions);

    HistoryEntry last = null;
    for (HistoryEntry logEntry : this.versionLog) {
      if (last != null) {
        Preconditions.checkArgument(
            (logEntry.timestampMillis() - last.timestampMillis()) >= 0,
            "[BUG] Expected sorted version log entries.");
      }
      last = logEntry;
    }
  }

  public String location() {
    return location;
  }

  public ViewDefinition definition() {
    return definition;
  }

  public Map<String, String> properties() {
    return properties;
  }

  public String property(String property, String defaultValue) {
    return properties.getOrDefault(property, defaultValue);
  }

  public int propertyAsInt(String property, int defaultValue) {
    String value = properties.get(property);
    if (value != null) {
      return Integer.parseInt(properties.get(property));
    }
    return defaultValue;
  }

  public Version version(int versionId) {
    return versionsById.get(versionId);
  }

  public Version currentVersion() {
    return versionsById.get(currentVersionId);
  }

  public int currentVersionId() {
    return currentVersionId;
  }

  public List<Version> versions() {
    return versions;
  }

  public List<HistoryEntry> history() {
    return versionLog;
  }

  private static Map<Integer, Version> indexVersions(List<Version> versions) {
    ImmutableMap.Builder<Integer, Version> builder = ImmutableMap.builder();
    for (Version version : versions) {
      builder.put(version.versionId(), version);
    }
    return builder.build();
  }

  public ViewVersionMetadata replaceProperties(Map<String, String> newProperties) {
    ValidationException.check(newProperties != null, "Cannot set properties to null");

    return new ViewVersionMetadata(
        location, definition, newProperties, currentVersionId, versions, versionLog);
  }
}
