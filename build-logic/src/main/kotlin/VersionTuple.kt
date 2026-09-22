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

import java.math.BigInteger
import java.nio.file.Files
import java.nio.file.Path
import java.util.regex.Pattern

/** Represents a semantic version with mandatory major, minor and patch numbers. */
data class VersionTuple(
  val major: Int,
  val minor: Int,
  val patch: Int,
  val prerelease: String? = null,
) : Comparable<VersionTuple> {

  companion object Factory {
    val pattern: Pattern =
      Pattern.compile(
        "^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)(?:-((?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\\.(?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\\+([0-9a-zA-Z-]+(?:\\.[0-9a-zA-Z-]+)*))?\$"
      )

    fun fromFile(file: Path): VersionTuple = create(Files.readString(file).trim())

    @JvmStatic
    fun create(string: String): VersionTuple {
      val matcher = pattern.matcher(string)
      if (!matcher.matches()) {
        throw IllegalArgumentException("'$string' is not a valid version string")
      }

      val major = matcher.group(1)
      val minor = matcher.group(2)
      val patch = matcher.group(3)
      val prerelease = matcher.group(4)
      val buildmetadata = matcher.group(5)

      if (buildmetadata != null) {
        throw IllegalArgumentException("Build metadata not supported")
      }

      return VersionTuple(major.toInt(), minor.toInt(), patch.toInt(), prerelease)
    }
  }

  val snapshot: Boolean
    get() = prerelease == "SNAPSHOT"

  fun bumpMajor(): VersionTuple = VersionTuple(major + 1, 0, 0)

  fun bumpMinor(): VersionTuple = VersionTuple(major, minor + 1, 0)

  fun bumpPatch(): VersionTuple = VersionTuple(major, minor, patch + 1)

  fun asSnapshot(): VersionTuple = VersionTuple(major, minor, patch, "SNAPSHOT")

  fun asRelease(): VersionTuple = VersionTuple(major, minor, patch)

  fun withPrerelease(value: String): VersionTuple = VersionTuple(major, minor, patch, value)

  fun writeToFile(file: Path): Path = Files.writeString(file, toString())

  override fun compareTo(other: VersionTuple): Int {
    var cmp: Int = major.compareTo(other.major)
    if (cmp != 0) {
      return cmp
    }

    cmp = minor.compareTo(other.minor)
    if (cmp != 0) {
      return cmp
    }

    cmp = patch.compareTo(other.patch)
    if (cmp != 0) {
      return cmp
    }

    if (prerelease == other.prerelease) {
      return 0
    }

    if (prerelease == null) {
      return 1
    }
    if (other.prerelease == null) {
      return -1
    }

    return comparePrerelease(prerelease, other.prerelease)
  }

  override fun toString(): String {
    return "$major.$minor.$patch${prerelease?.let { "-$it" } ?: ""}"
  }

  private fun comparePrerelease(left: String, right: String): Int {
    val leftIdentifiers = left.split('.')
    val rightIdentifiers = right.split('.')

    for (i in 0 until minOf(leftIdentifiers.size, rightIdentifiers.size)) {
      val leftIdentifier = leftIdentifiers[i]
      val rightIdentifier = rightIdentifiers[i]
      if (leftIdentifier == rightIdentifier) {
        continue
      }

      val leftNumeric = leftIdentifier.all(Char::isDigit)
      val rightNumeric = rightIdentifier.all(Char::isDigit)
      val comparison =
        when {
          leftNumeric && rightNumeric ->
            BigInteger(leftIdentifier).compareTo(BigInteger(rightIdentifier))
          leftNumeric -> -1
          rightNumeric -> 1
          else -> leftIdentifier.compareTo(rightIdentifier)
        }
      if (comparison != 0) {
        return comparison
      }
    }

    return leftIdentifiers.size.compareTo(rightIdentifiers.size)
  }
}
