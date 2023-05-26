/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.versioned;

import static com.google.common.base.Preconditions.checkArgument;
import static org.projectnessie.model.Validation.RELATIVE_COMMIT_SPEC_PART_PATTERN;
import static org.projectnessie.versioned.RelativeCommitSpec.Type.TIMESTAMP_MILLIS_EPOCH;
import static org.projectnessie.versioned.RelativeCommitSpec.Type.typeForMnemonic;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import org.immutables.value.Value;

@Value.Immutable
public abstract class RelativeCommitSpec {
  @Value.Parameter(order = 1)
  public abstract Type type();

  /** Value for {@link Type#N_TH_PREDECESSOR} and {@link Type#N_TH_PARENT}. */
  @Value.Parameter(order = 2)
  public abstract long longValue();

  /** Value for {@link Type#TIMESTAMP_MILLIS_EPOCH}. */
  @Value.Parameter(order = 3)
  public abstract Instant instantValue();

  @Override
  public String toString() {
    if (type() != TIMESTAMP_MILLIS_EPOCH) {
      return Character.toString(type().mnemonic()) + longValue();
    }
    return type().mnemonic() + instantValue().toString();
  }

  public static List<RelativeCommitSpec> parseRelativeSpecs(String relativeSpec) {
    Matcher relatives = RELATIVE_COMMIT_SPEC_PART_PATTERN.matcher(relativeSpec);
    List<RelativeCommitSpec> relativeSpecs = new ArrayList<>();
    while (relatives.find()) {
      String type = relatives.group(1);
      String num = relatives.group(2);
      if (type == null && num == null) {
        break;
      }
      // Can be sure that type.length()==1
      @SuppressWarnings("DataFlowIssue")
      char mnemonic = type.charAt(0);
      relativeSpecs.add(relativeCommitSpec(mnemonic, num));
    }
    return relativeSpecs;
  }

  public static RelativeCommitSpec relativeCommitSpec(char mnemonic, String value) {
    return relativeCommitSpec(typeForMnemonic(mnemonic), value);
  }

  public static RelativeCommitSpec relativeCommitSpec(Type type, String value) {
    long num = 0L;
    Instant instant = Instant.EPOCH;

    switch (type) {
      case TIMESTAMP_MILLIS_EPOCH:
        try {
          long v = Long.parseLong(value);
          instant = Instant.ofEpochMilli(v);
        } catch (NumberFormatException e1) {
          try {
            instant = Instant.parse(value);
          } catch (DateTimeParseException e2) {
            throw new IllegalArgumentException(
                "'*' relative commit spec argument '"
                    + value
                    + "' must be represent an ISO-8601 timestamp ('Z' only) or a numeric value representing the milliseconds since epoch");
          }
        }
        break;
      case N_TH_PARENT:
      case N_TH_PREDECESSOR:
        try {
          num = Long.parseLong(value);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(e.getMessage());
        }
        checkArgument(
            num > 0 && num < (long) Integer.MAX_VALUE,
            "Illegal value %s for spec type %s (%s)",
            value,
            type.name(),
            type.mnemonic());
        break;
      default:
        throw new UnsupportedOperationException();
    }

    return relativeCommitSpec(type, num, instant);
  }

  public static RelativeCommitSpec relativeCommitSpec(Type type, long num, Instant instant) {
    return ImmutableRelativeCommitSpec.of(type, num, instant);
  }

  public enum Type {
    TIMESTAMP_MILLIS_EPOCH('*'),
    N_TH_PREDECESSOR('~'),
    N_TH_PARENT('^');

    static Type typeForMnemonic(char mnemonic) {
      switch (mnemonic) {
        case '*':
          return TIMESTAMP_MILLIS_EPOCH;
        case '~':
          return N_TH_PREDECESSOR;
        case '^':
          return N_TH_PARENT;
        default:
          throw new IllegalArgumentException("Illegal mnemonic '" + mnemonic + '\'');
      }
    }

    private final char mnemonic;

    Type(char mnemonic) {
      this.mnemonic = mnemonic;
    }

    public char mnemonic() {
      return mnemonic;
    }
  }
}
