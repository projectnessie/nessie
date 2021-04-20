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
package org.projectnessie.model;

import java.util.List;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableField.class)
@JsonDeserialize(as = ImmutableField.class)
public interface Field {

  enum FieldType {
    NULL,
    BOOLEAN,
    INT8,
    UINT8,
    INT16,
    UINT16,
    INT32,
    UINT32,
    INT64,
    UINT64,
    FLOAT16,
    FLOAT32,
    FLOAT64,
    BINARY,
    FIXED_BINARY,
    LARGE_BINARY,
    UTF8,
    LARGE_UTF8,
    DATE,
    TIME,
    TIMESTAMP,
    DURATION,
    INTERVAL,
    DECIMAL,
    LIST,
    LARGE_LIST,
    FIXED_LIST,
    MAP,
    STRUCT,
    DENSE_UNION,
    SPARSE_UNION
  }

  enum TimeUnit {
    SECOND,
    MILLISECOND,
    MICROSECOND,
    NANOSECOND
  }

  enum DateUnit {
    DAY,
    MILLISECOND
  }

  enum IntervalUnit {
    YEAR_MONTH,
    DAY_TIME
  }

  FieldType getType();

  String getName();

  List<Field> getChildren();

}
