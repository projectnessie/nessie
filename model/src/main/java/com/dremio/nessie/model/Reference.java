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
package com.dremio.nessie.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.DiscriminatorMapping;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    type = SchemaType.OBJECT,
    title = "Reference",
    oneOf = {Branch.class, Tag.class, Hash.class},
    discriminatorMapping = {
      @DiscriminatorMapping(value = "TAG", schema = Tag.class),
      @DiscriminatorMapping(value = "BRANCH", schema = Branch.class),
      @DiscriminatorMapping(value = "HASH", schema = Hash.class)
    },
    discriminatorProperty = "type")
@JsonSubTypes({@Type(Branch.class), @Type(Tag.class), @Type(Hash.class)})
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
public interface Reference extends Base {
  /** Human readable reference name. */
  String getName();

  /** backend system id. Usually the 20-byte hash of the commit this reference points to. */
  @Nullable
  String getHash();
}
