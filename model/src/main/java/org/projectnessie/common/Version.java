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
package org.projectnessie.common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Container for a version number. */
public class Version implements Comparable<Version> {

  private static final Pattern PARSE_PATTERN =
      Pattern.compile("^([0-9]+)([.]([0-9]+)([.]([0-9]+))?)?(-SNAPSHOT)?");

  private final int major;
  private final int minor;
  private final int patch;
  private final boolean snapshot;
  private final String str;

  /**
   * Construct a new version instance.
   *
   * @param major major version number
   * @param minor minor version number
   * @param patch patch version number
   * @param snapshot snapshot-version flag
   */
  public Version(int major, int minor, int patch, boolean snapshot) {
    this.major = major;
    this.minor = minor;
    this.patch = patch;
    this.snapshot = snapshot;
    this.str = String.format("%d.%d.%d%s", major, minor, patch, snapshot ? "-SNAPSHOT" : "");
  }

  /**
   * Parses a version string that matches the pattern {@link #PARSE_PATTERN}.
   *
   * @param version version string to parse
   * @return parsed version
   * @throws IllegalArgumentException if {@code version} is not a valid version string
   */
  public static Version parse(String version) {
    if (version == null) {
      throw new NullPointerException("null version argument");
    }

    Matcher m = PARSE_PATTERN.matcher(version);
    if (!m.matches()) {
      throw new IllegalArgumentException("Not a valid version string: " + version);
    }

    String strMajor = m.group(1);
    String strMinor = m.group(3);
    String strPatch = m.group(5);
    String strSnapshot = m.group(6);

    try {
      int major = Integer.parseInt(strMajor);
      int minor = strMinor != null ? Integer.parseInt(strMinor) : 0;
      int patch = strPatch != null ? Integer.parseInt(strPatch) : 0;
      boolean snapshot = "-SNAPSHOT".equals(strSnapshot);

      return new Version(major, minor, patch, snapshot);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Not a valid version string: " + version, e);
    }
  }

  @Override
  public String toString() {
    return str;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Version version = (Version) o;
    return major == version.major
        && minor == version.minor
        && patch == version.patch
        && snapshot == version.snapshot;
  }

  @Override
  public int hashCode() {
    return str.hashCode();
  }

  @Override
  public int compareTo(Version o) {
    int r;
    r = Integer.compare(this.major, o.major);
    if (r != 0) {
      return r;
    }
    r = Integer.compare(this.minor, o.minor);
    if (r != 0) {
      return r;
    }
    r = Integer.compare(this.patch, o.patch);
    if (r != 0) {
      return r;
    }
    return Integer.compare(this.snapshot ? 0 : 1, o.snapshot ? 0 : 1);
  }

  public Version removeSnapshot() {
    return this.snapshot ? new Version(major, minor, patch, false) : this;
  }
}
