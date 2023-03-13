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
package org.apache.iceberg.view;

/**
 * View operations that lead to a new version of view getting created.
 *
 * <p>A version can return the operation that resulted in creating that version of the view. Users
 * can inspect the operation to get more information in case a rollback is desired.
 */
public class DDLOperations {
  private DDLOperations() {}

  /** The view was created. */
  public static final String CREATE = "create";

  /**
   * View definition was replaced. This operation covers actions such as associating a different
   * schema with the view, adding a column comment etc.
   */
  public static final String REPLACE = "replace";

  /** View column comments were altered. */
  public static final String ALTER_COMMENT = "alter-comment";
}
