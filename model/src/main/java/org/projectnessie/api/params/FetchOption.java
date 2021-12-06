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
package org.projectnessie.api.params;

import javax.annotation.Nullable;

public enum FetchOption {
  MINIMAL,
  ALL;

  /**
   * Will return <code>true</code> if the given <code>fetchOption</code> is {@link FetchOption#ALL},
   * <code>false</code> otherwise.
   *
   * @param fetchOption the {@link FetchOption} to evaluate.
   * @return <code>true</code> if the given <code>fetchOption</code> is {@link FetchOption#ALL},
   *     <code>false</code> otherwise.
   */
  public static boolean isFetchAll(@Nullable FetchOption fetchOption) {
    return ALL == fetchOption;
  }

  /**
   * Will return the name from the given <code>fetchOption</code> or {@link FetchOption#MINIMAL} if
   * the given <code>fetchOption</code> is <code>null</code>.
   *
   * @param fetchOption The {@link FetchOption} to evaluate and potentially return.
   * @return The name of the given <code>fetchOption</code> or {@link FetchOption#MINIMAL} if the
   *     given <code>fetchOption</code> is <code>null</code>
   */
  public static String getFetchOptionName(@Nullable FetchOption fetchOption) {
    return null == fetchOption ? FetchOption.MINIMAL.name() : fetchOption.name();
  }
}
