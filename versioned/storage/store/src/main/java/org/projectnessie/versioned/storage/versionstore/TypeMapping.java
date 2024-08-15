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
package org.projectnessie.versioned.storage.versionstore;

import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.time.format.DateTimeFormatter.ISO_ZONED_DATE_TIME;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.DAY_OF_WEEK;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.OFFSET_SECONDS;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;
import static java.util.Collections.emptyList;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.newCommitHeaders;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.SignStyle;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.logic.CreateCommit;
import org.projectnessie.versioned.storage.common.objtypes.CommitHeaders;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;

public final class TypeMapping {

  public static final String MAIN_UNIVERSE = "M";
  public static final String CONTENT_DISCRIMINATOR = "C";

  public static final String COMMIT_TIME = "date";
  public static final String AUTHOR_TIME = "author-date";
  public static final String SIGNED_OFF_BY = "signed-off-by";
  public static final String COMMITTER = "committer";
  public static final String AUTHOR = "author";

  private TypeMapping() {}

  @Nonnull
  public static ObjId hashToObjId(@Nonnull Hash hash) {
    return ObjId.objIdFromBytes(hash.asBytes());
  }

  @Nonnull
  public static Hash objIdToHash(@Nonnull ObjId objId) {
    return Hash.of(objId.asBytes());
  }

  /**
   * Converts a {@link StoreKey} to a {@link ContentKey}, returning {@code null}, if the store key
   * does not reference the {@link #MAIN_UNIVERSE main universe} or not a {@link
   * #CONTENT_DISCRIMINATOR content object}.
   *
   * <p>A {@link ContentKey} is represented as a {@link StoreKey} as follows:<br>
   * {@code universe CHAR_0 key-element ( CHAR_1 key-element ) * CHAR_0 variant}
   *
   * <p>So it is the "universe" followed by {@code (char)0} followed by the key-elements, separated
   * by {@code (char)1}, followed by {@code (char)0}, followed by the "variant".
   */
  @Nullable
  public static ContentKey storeKeyToKey(@Nonnull StoreKey storeKey) {
    String universe;
    List<String> keyElements;
    String variant;

    String raw = storeKey.rawString();
    int idx1 = raw.indexOf((char) 0);
    if (idx1 == -1) {
      throw new IllegalArgumentException("Unsupported StoreKey '" + storeKey + "'");
    }
    universe = raw.substring(0, idx1);
    int idx2 = raw.indexOf((char) 0, idx1 + 1);
    if (idx2 == -1) {
      keyElements = emptyList();
      variant = raw.substring(idx1 + 1);
    } else {

      keyElements = new ArrayList<>(10);
      int off = idx1 + 1;
      for (int i = off; i < idx2; i++) {
        char c = raw.charAt(i);
        if (c == (char) 1) {
          keyElements.add(raw.substring(off, i));
          off = i + 1;
        }
      }
      keyElements.add(raw.substring(off, idx2));
      variant = raw.substring(idx2 + 1);
    }

    if (!universe.equals(MAIN_UNIVERSE) || !CONTENT_DISCRIMINATOR.equals(variant)) {
      return null;
    }
    return ContentKey.of(keyElements);
  }

  /**
   * Converts a {@link ContentKey} to a {@link StoreKey} in the {@link #MAIN_UNIVERSE main universe}
   * as a {@link #CONTENT_DISCRIMINATOR content object}.
   */
  @Nonnull
  public static StoreKey keyToStoreKey(@Nonnull ContentKey key) {
    return keyToStoreKeyVariant(key, CONTENT_DISCRIMINATOR);
  }

  @Nonnull
  public static StoreKey keyToStoreKey(@Nonnull List<String> keyElements) {
    return keyToStoreKeyVariant(keyElements, CONTENT_DISCRIMINATOR);
  }

  @Nonnull
  public static StoreKey keyToStoreKeyNoVariant(@Nonnull ContentKey key) {
    return StoreKey.keyFromString(keyToStoreKeyPrepare(key).toString());
  }

  @Nonnull
  public static StoreKey keyToStoreKeyVariant(@Nonnull ContentKey key, String discriminator) {
    return keyToStoreKeyVariant(key.getElements(), discriminator);
  }

  @Nonnull
  public static StoreKey keyToStoreKeyVariant(
      @Nonnull List<String> keyElements, String discriminator) {
    StringBuilder sb = keyToStoreKeyPrepare(keyElements);
    sb.append((char) 0).append(discriminator);
    return StoreKey.keyFromString(sb.toString());
  }

  @Nonnull
  public static StoreKey keyToStoreKeyMin(@Nonnull ContentKey key) {
    StringBuilder sb = keyToStoreKeyPrepare(key);
    return StoreKey.keyFromString(sb.toString());
  }

  /** Computes the max-key. */
  @Nonnull
  public static StoreKey keyToStoreKeyMax(@Nonnull ContentKey key) {
    StringBuilder sb = keyToStoreKeyPrepare(key);
    sb.append((char) 0).append((char) 255);
    return StoreKey.keyFromString(sb.toString());
  }

  @Nonnull
  private static StringBuilder keyToStoreKeyPrepare(@Nonnull ContentKey key) {
    return keyToStoreKeyPrepare(key.getElements());
  }

  @Nonnull
  private static StringBuilder keyToStoreKeyPrepare(@Nonnull List<String> keyElements) {
    // Note: the relative values of outer and inner (key elements) separators affect the correctness
    // of StoreKey comparisons WRT to ContentKey comparisons. The inner separator must be greater
    // than the outer separator because longer ContentKeys are greater than shorter ContentKeys.
    int len = 4; // MAIN_UNIVERSE + separator + separator + discriminator
    int num = keyElements.size();
    for (String keyElement : keyElements) {
      len += keyElement.length() + 1;
    }
    StringBuilder sb = new StringBuilder(len);
    sb.append(MAIN_UNIVERSE);
    if (num > 0) {
      sb.append((char) 0);
      for (int i = 0; i < num; i++) {
        if (i > 0) {
          sb.append((char) 1);
        }
        sb.append(keyElements.get(i));
      }
    }
    return sb;
  }

  @Nonnull
  public static CommitHeaders headersFromCommitMeta(@Nonnull CommitMeta commitMeta) {
    CommitHeaders.Builder headers = newCommitHeaders();
    commitMeta.getAllProperties().forEach((k, v) -> v.forEach(s -> headers.add(k, s)));
    if (commitMeta.getCommitTime() != null) {
      headers.add(COMMIT_TIME, instantToHeaderValue(commitMeta.getCommitTime()));
    }
    if (commitMeta.getAuthorTime() != null) {
      headers.add(AUTHOR_TIME, instantToHeaderValue(commitMeta.getAuthorTime()));
    }
    if (commitMeta.getCommitter() != null) {
      headers.add(COMMITTER, commitMeta.getCommitter());
    }
    commitMeta.getAllAuthors().forEach(a -> headers.add(AUTHOR, a));
    commitMeta.getAllSignedOffBy().forEach(a -> headers.add(SIGNED_OFF_BY, a));
    return headers.build();
  }

  public static ImmutableCommitMeta.Builder headersToCommitMeta(
      @Nonnull CommitHeaders headers, @Nonnull ImmutableCommitMeta.Builder commitMeta) {
    for (String header : headers.keySet()) {
      switch (header) {
        case AUTHOR:
          commitMeta.addAllAllAuthors(headers.getAll(header));
          break;
        case COMMITTER:
          commitMeta.committer(headers.getFirst(header));
          break;
        case SIGNED_OFF_BY:
          commitMeta.addAllAllSignedOffBy(headers.getAll(header));
          break;
        case COMMIT_TIME:
          applyInstant(headers.getFirst(header), commitMeta::commitTime);
          break;
        case AUTHOR_TIME:
          applyInstant(headers.getFirst(header), commitMeta::authorTime);
          break;
        default:
          commitMeta.putAllProperties(header, headers.getAll(header));
          break;
      }
    }
    return commitMeta;
  }

  public static void fromCommitMeta(
      @Nonnull CommitMeta commitMeta, @Nonnull CreateCommit.Builder commit) {
    commit.headers(headersFromCommitMeta(commitMeta)).message(commitMeta.getMessage());
  }

  @Nonnull
  public static CommitMeta toCommitMeta(@Nonnull CommitObj commit) {
    ImmutableCommitMeta.Builder commitMeta =
        CommitMeta.builder()
            .message(commit.message())
            .hash(commit.id().toString())
            .addParentCommitHashes(commit.directParent().toString());
    commit.secondaryParents().forEach(p -> commitMeta.addParentCommitHashes(p.toString()));
    headersToCommitMeta(commit.headers(), commitMeta);
    return commitMeta.build();
  }

  private static void applyInstant(String v, Consumer<Instant> t) {
    try {
      t.accept(headerValueToInstant(v));
    } catch (DateTimeParseException ignore) {
      // There's nothing we could do here to "repair" it or deal with it. The value's not
      // gone, just not available via 'CommitMeta'. Could happen, if a string is added
      // directly via 'CommitHeaders' but the value's read via 'CommitMeta'.
    }
  }

  private static final DateTimeFormatter LENIENT_GIT_DATE_TIME;

  private static final DateTimeFormatter LENIENT_RFC_1123_DATE_TIME;

  static {
    Map<Long, String> dow = new HashMap<>();
    dow.put(1L, "Mon");
    dow.put(2L, "Tue");
    dow.put(3L, "Wed");
    dow.put(4L, "Thu");
    dow.put(5L, "Fri");
    dow.put(6L, "Sat");
    dow.put(7L, "Sun");
    Map<Long, String> moy = new HashMap<>();
    moy.put(1L, "Jan");
    moy.put(2L, "Feb");
    moy.put(3L, "Mar");
    moy.put(4L, "Apr");
    moy.put(5L, "May");
    moy.put(6L, "Jun");
    moy.put(7L, "Jul");
    moy.put(8L, "Aug");
    moy.put(9L, "Sep");
    moy.put(10L, "Oct");
    moy.put(11L, "Nov");
    moy.put(12L, "Dec");

    LENIENT_GIT_DATE_TIME =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .parseLenient()
            .optionalStart()
            .appendText(DAY_OF_WEEK, dow)
            .appendLiteral(' ')
            .optionalEnd()
            .appendText(MONTH_OF_YEAR, moy)
            .appendLiteral(' ')
            .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NOT_NEGATIVE)
            .appendLiteral(' ')
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .optionalEnd()
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .appendLiteral(' ')
            .appendValue(YEAR, 4) // 2 digit year not handled
            .optionalStart()
            .appendLiteral(' ')
            .appendOffset("+HHMM", "GMT") // should handle UT/Z/EST/EDT/CST/CDT/MST/MDT/PST/MDT
            .optionalEnd()
            .parseDefaulting(OFFSET_SECONDS, 0)
            .toFormatter();

    LENIENT_RFC_1123_DATE_TIME =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .parseLenient()
            .optionalStart()
            .appendText(DAY_OF_WEEK, dow)
            .appendLiteral(", ")
            .optionalEnd()
            .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NOT_NEGATIVE)
            .appendLiteral(' ')
            .appendText(MONTH_OF_YEAR, moy)
            .appendLiteral(' ')
            .appendValue(YEAR, 4) // 2 digit year not handled
            .appendLiteral(' ')
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .optionalEnd()
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .optionalStart()
            .appendLiteral(' ')
            .appendOffset("+HHMM", "GMT") // should handle UT/Z/EST/EDT/CST/CDT/MST/MDT/PST/MDT
            .optionalEnd()
            .parseDefaulting(OFFSET_SECONDS, 0)
            .toFormatter();
  }

  private static final DateTimeFormatter[] DT_FORMATTERS =
      new DateTimeFormatter[] {
        ISO_OFFSET_DATE_TIME, LENIENT_GIT_DATE_TIME, LENIENT_RFC_1123_DATE_TIME, ISO_ZONED_DATE_TIME
      };

  public static Instant headerValueToInstant(String v) {
    DateTimeParseException fail = null;
    for (DateTimeFormatter formatter : DT_FORMATTERS) {
      try {
        ZonedDateTime zdt = ZonedDateTime.parse(v, formatter);
        return zdt.toInstant();
      } catch (DateTimeParseException e) {
        if (fail == null) {
          fail = e;
        } else {
          fail.addSuppressed(e);
        }
      }
    }
    throw fail;
  }

  public static String instantToHeaderValue(Instant v) {
    return v.toString();
  }
}
