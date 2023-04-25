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
package org.projectnessie.events.api;

/**
 * An enum of all possible content types.
 *
 * <p>Please note that this enum may evolve in the future, and more enum values may be added. It is
 * important that SPI implementers be prepared to handle unknown enum values.
 */
public enum ContentType {
  /**
   * The content is a namespace.
   *
   * @see Namespace
   */
  NAMESPACE(Namespace.class),

  /**
   * The content is an Iceberg table.
   *
   * @see IcebergTable
   */
  ICEBERG_TABLE(IcebergTable.class),

  /**
   * The content is an Iceberg view.
   *
   * @see IcebergView
   */
  ICEBERG_VIEW(IcebergView.class),

  /**
   * The content is a Delta Lake table.
   *
   * @see DeltaLakeTable
   */
  DELTA_LAKE_TABLE(DeltaLakeTable.class),

  /**
   * The content is a custom type. This type is a catch-all type for all other runtime types that
   * are not one of the built-in types above.
   *
   * @see CustomContent
   */
  CUSTOM(CustomContent.class),
  ;

  private final Class<? extends Content> subtype;

  ContentType(Class<? extends Content> subtype) {
    this.subtype = subtype;
  }

  public Class<? extends Content> getSubtype() {
    return subtype;
  }
}
