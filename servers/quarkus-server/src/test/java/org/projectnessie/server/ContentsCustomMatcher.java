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
package org.projectnessie.server;

import java.util.Objects;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.projectnessie.model.Contents;
import org.projectnessie.model.IcebergTable;

public class ContentsCustomMatcher  extends TypeSafeMatcher<Contents> {
  private final Contents expected;

  private ContentsCustomMatcher(Contents expected) {
    this.expected = expected;
  }

  @Override
  protected boolean matchesSafely(Contents item) {
    if (item instanceof IcebergTable && expected instanceof IcebergTable) {
      return Objects.equals(item.getId(), expected.getId())
        && Objects.equals(item.getPermanentId(), expected.getPermanentId())
        && ((IcebergTable) item).getMetadataLocation().equals(((IcebergTable) expected).getMetadataLocation());
    }
    return false;
  }

  @Override
  public void describeTo(Description description) {
    description.appendText("match contents is equal. Except permanentId which should be unequal");
  }

  public static Matcher<Contents> equalTo(Contents expected) {
    return new ContentsCustomMatcher(expected);
  }
}
