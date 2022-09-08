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
package org.projectnessie.s3mock.data;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.immutables.value.Value;

@JsonSerialize(as = ImmutableRange.class)
@JsonDeserialize(as = ImmutableRange.class)
@Value.Immutable
public interface Range {

  String REQUESTED_RANGE_REGEXP = "^bytes=((\\d*)-(\\d*))((,\\d*-\\d*)*)";

  Pattern REQUESTED_RANGE_PATTERN = Pattern.compile(REQUESTED_RANGE_REGEXP);

  long start();

  long end();

  @SuppressWarnings("unused") // JAX-RS factory function
  static Range fromString(String rangeString) {
    requireNonNull(rangeString);

    final Range range;

    // parsing a range specification of format: "bytes=start-end" - multiple ranges not supported
    rangeString = rangeString.trim();
    final Matcher matcher = REQUESTED_RANGE_PATTERN.matcher(rangeString);
    if (matcher.matches()) {
      final String rangeStart = matcher.group(2);
      final String rangeEnd = matcher.group(3);

      range =
          ImmutableRange.builder()
              .start(rangeStart == null ? 0L : Long.parseLong(rangeStart))
              .end(rangeEnd.isEmpty() ? Long.MAX_VALUE : Long.parseLong(rangeEnd))
              .build();

      if (matcher.groupCount() == 5 && !"".equals(matcher.group(4))) {
        throw new IllegalArgumentException(
            "Unsupported range specification. Only single range specifications allowed");
      }

      return range;
    }
    throw new IllegalArgumentException(
        "Range header is malformed. Only bytes supported as range type.");
  }

  @Value.Check
  default void check() {
    if (start() < 0) {
      throw new IllegalArgumentException(
          "Unsupported range specification. A start byte must be supplied");
    }

    if (end() != -1 && end() < start()) {
      throw new IllegalArgumentException(
          "Range header is malformed. End byte is smaller than start byte.");
    }
  }
}
