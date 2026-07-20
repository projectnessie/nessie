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
package org.projectnessie.objectstoragemock.sts;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nullable;
import java.time.Instant;
import org.immutables.value.Value;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonSerialize;
import tools.jackson.databind.ext.javatime.deser.InstantDeserializer;
import tools.jackson.databind.ext.javatime.ser.InstantSerializer;
import tools.jackson.databind.util.StdDateFormat;

@JsonSerialize(as = ImmutableCredentials.class)
@JsonDeserialize(as = ImmutableCredentials.class)
@Value.Immutable
@Value.Style(jdkOnly = true)
public interface Credentials {

  @JsonProperty("AccessKeyId")
  String accessKeyId();

  @JsonProperty("SecretAccessKey")
  String secretAccessKey();

  @JsonProperty("SessionToken")
  @Nullable
  String sessionToken();

  @JsonProperty("Expiration")
  @JsonFormat(
      shape = JsonFormat.Shape.STRING,
      pattern = StdDateFormat.DATE_FORMAT_STR_ISO8601,
      timezone = "UTC")
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  Instant expiration();
}
