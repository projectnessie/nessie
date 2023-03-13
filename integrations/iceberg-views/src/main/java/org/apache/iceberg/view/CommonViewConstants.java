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

public class CommonViewConstants {
  /**
   * Constants that can be used by engines to send additional information for view operations being
   * performed.
   */
  public static final String COMMON_VIEW = "common_view";

  public static final String ENGINE_VERSION = "engine_version";
  public static final String GENIE_ID = "genie_id";
  public static final String OPERATION = "operation";

  /**
   * All the properties except 'common_view' are stored in the View's Version Summary. 'operation'
   * is supplied by the library and hence does not need to appear in the enum below. If you add a
   * new constant that is specific to a version of the view, make sure to add it to the enum below.
   */
  protected enum ViewVersionSummaryConstants {
    engine_version,
    genie_id
  }
}
