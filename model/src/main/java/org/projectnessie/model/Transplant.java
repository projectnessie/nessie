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

import static org.projectnessie.model.Validation.validateHash;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.media.SchemaProperty;
import org.immutables.value.Value;

@Schema(
    type = SchemaType.OBJECT,
    title = "Transplant",
    // Smallrye does neither support JsonFormat nor javax.validation.constraints.Pattern :(
    properties = {
      @SchemaProperty(name = "fromRefName", pattern = Validation.REF_NAME_REGEX),
      @SchemaProperty(name = "hashesToTransplant", uniqueItems = true)
    })
@Value.Immutable
@JsonSerialize(as = ImmutableTransplant.class)
@JsonDeserialize(as = ImmutableTransplant.class)
public interface Transplant {

  @NotNull
  @Size(min = 1)
  List<String> getHashesToTransplant();

  @NotBlank
  @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
  String getFromRefName();

  /**
   * Validation rule using {@link org.projectnessie.model.Validation#validateHash(String)}
   * (String)}.
   */
  @Value.Check
  default void checkHashes() {
    List<String> hashes = getHashesToTransplant();
    if (hashes != null) {
      for (String hash : hashes) {
        validateHash(hash);
      }
    }
  }
}
