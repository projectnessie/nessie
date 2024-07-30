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
package org.projectnessie.catalog.service.rest;

import static java.time.format.DateTimeFormatter.ISO_INSTANT;
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

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.TemporalAccessor;
import java.util.HashMap;
import java.util.Map;

public final class TimestampParser {
  private TimestampParser() {}

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

  /**
   * Returns a "relative commit spec" according to {@code
   * org.projectnessie.versioned.RelativeCommitSpec.Type}.
   */
  public static String timestampToNessie(String timestamp) {
    try {
      Long.parseLong(timestamp);
      return "*" + timestamp;
    } catch (Exception ignore) {
    }
    for (DateTimeFormatter fmt : DT_FORMATTERS) {
      try {
        TemporalAccessor instant = fmt.parse(timestamp);
        return "*" + ISO_INSTANT.format(instant);
      } catch (Exception ignore) {
      }
    }
    throw new IllegalArgumentException(
        "Invalid timestamp value '"
            + timestamp
            + "', must be an ISO 8601 instant, an ISO instant with zone offset, or the number of milliseconds since epoch.");
  }
}
