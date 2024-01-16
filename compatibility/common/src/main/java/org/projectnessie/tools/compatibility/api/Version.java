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
  public static final Version NEW_STORAGE_MODEL_WITH_COMPAT_TESTING =
      Version.parseVersion("0.55.0");
  public static final Version SPEC_VERSION_IN_CONFIG_V2 = Version.parseVersion("0.55.0");
  public static final Version SPEC_VERSION_IN_CONFIG_V2_SEMVER = Version.parseVersion("0.57.0");
  public static final Version SPEC_VERSION_IN_CONFIG_V2_GA = Version.parseVersion("0.59.0");
  public static final Version ACTUAL_VERSION_IN_CONFIG_V2 = Version.parseVersion("0.59.0");

  /** See <a href="https://github.com/projectnessie/nessie/pull/6894">PR #6894</a>. */
  public static final Version MERGE_KEY_BEHAVIOR_FIX = Version.parseVersion("0.59.1");

  /** See <a href="https://github.com/projectnessie/nessie/pull/7854">PR #7854</a>. */
  public static final Version NESSIE_URL_API_SUFFIX = Version.parseVersion("0.75.0");

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
