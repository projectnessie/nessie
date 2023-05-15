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
package org.projectnessie.tools.compatibility.api;

import java.util.Arrays;
import java.util.Objects;

/**
 * Value object representing a released nessie version or "virtual" versions {@value
 * #CURRENT_STRING} or {@value #NOT_CURRENT_STRING}.
 */
public class Version implements Comparable<Version> {

  public static final Version CLIENT_RESULTS_NATIVE_STREAM = Version.parseVersion("0.31.0");
  public static final Version HAS_MERGE_RESPONSE = Version.parseVersion("0.31.0");
  public static final Version VERSIONED_REST_URI_START = Version.parseVersion("0.46.0");
  public static final Version REFLOG_FOR_COMMIT_REMOVED = Version.parseVersion("0.44.0");
  // OPENTRACING_VERSION_MISMATCH_* is the version range where Nessie declared dependencies on
  // incompatible versions of some OpenTracing artifacts.
  public static final Version OPENTRACING_VERSION_MISMATCH_LOW = Version.parseVersion("0.40.0");
  public static final Version OPENTRACING_VERSION_MISMATCH_HIGH = Version.parseVersion("0.42.0");
  // CLIENT_LOG4J_UNDECLARED_* is the version range where :nessie-client uses log4j without
  // declaring an explicit dependency in its POM.
  public static final Version CLIENT_LOG4J_UNDECLARED_LOW = Version.parseVersion("0.46.0");
  public static final Version API_V2 = Version.parseVersion("0.46.0");
  public static final Version CLIENT_LOG4J_UNDECLARED_HIGH = Version.parseVersion("0.47.1");
  // COMPAT_COMMON_DEPENDENCIES_START is the version where dependency declarations for
  // "compatibility" tests moved to :nessie-compatibility-common
  public static final Version COMPAT_COMMON_DEPENDENCIES_START = Version.parseVersion("0.48.2");
  public static final Version OLD_GROUP_IDS = Version.parseVersion("0.50.0");
  public static final Version SPEC_VERSION_IN_CONFIG_V2 = Version.parseVersion("0.55.0");
  public static final Version SPEC_VERSION_IN_CONFIG_V2_SEMVER = Version.parseVersion("0.57.0");
  public static final Version ACTUAL_VERSION_IN_CONFIG_V2 = Version.parseVersion("0.59.0");

  public static final String CURRENT_STRING = "current";
  public static final String NOT_CURRENT_STRING = "not-current";
  private final int[] tuple;

  /**
   * Useful to run an upgrade test only for the in-tree version, when used in
   * {@code @VersionCondition(minVersion = "current")}.
   */
  public static final Version CURRENT = new Version(new int[] {Integer.MAX_VALUE});

  /**
   * Useful to exclude the {@link #CURRENT} version using {@code @VersionCondition(maxVersion =
   * "not-current")}.
   */
  public static final Version NOT_CURRENT = new Version(new int[] {Integer.MAX_VALUE - 1});

  private Version(int[] tuple) {
    this.tuple = tuple;
  }

  public static Version parseVersion(String version) {
    if (CURRENT_STRING.equalsIgnoreCase(version)) {
      return CURRENT;
    }
    if (NOT_CURRENT_STRING.equalsIgnoreCase(version)) {
      return NOT_CURRENT;
    }
    Objects.requireNonNull(version, "Version mut not be null");

    int[] t = new int[8];
    int len = 0;
    while (true) {
      int i = version.indexOf('.');
      if (i == -1) {
        break;
      }
      t[len++] = parsePart(version.substring(0, i));
      version = version.substring(i + 1);
    }
    t[len++] = parsePart(version);
    return new Version(Arrays.copyOf(t, len));
  }

  private static int parsePart(String part) {
    int val;
    try {
      val = Integer.parseInt(part);
      if (val < 0 || val >= Integer.MAX_VALUE - 1) {
        throw new NumberFormatException();
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid version number part: " + part);
    }
    return val;
  }

  public boolean isGreaterThan(Version version) {
    return this.compareTo(version) > 0;
  }

  public boolean isGreaterThanOrEqual(Version version) {
    return this.compareTo(version) >= 0;
  }

  public boolean isLessThan(Version version) {
    return this.compareTo(version) < 0;
  }

  public boolean isLessThanOrEqual(Version version) {
    return this.compareTo(version) <= 0;
  }

  public boolean isSame(Version version) {
    return this.compareTo(version) == 0;
  }

  @Override
  public int compareTo(Version o) {
    for (int i = 0; i < tuple.length || i < o.tuple.length; i++) {
      int mine = i < tuple.length ? tuple[i] : 0;
      int other = i < o.tuple.length ? o.tuple[i] : 0;
      int r = Integer.compare(mine, other);
      if (r != 0) {
        return r;
      }
    }
    return 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Version)) {
      return false;
    }
    Version version = (Version) o;
    return Arrays.equals(tuple, version.tuple);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(tuple);
  }

  @Override
  public String toString() {
    if (CURRENT.equals(this)) {
      return CURRENT_STRING;
    }
    if (NOT_CURRENT.equals(this)) {
      return NOT_CURRENT_STRING;
    }

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < tuple.length; i++) {
      if (i > 0) {
        sb.append('.');
      }
      sb.append(tuple[i]);
    }
    return sb.toString();
  }
}
