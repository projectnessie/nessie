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

package com.dremio.nessie.server;

import javax.annotation.Nonnull;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import com.dremio.nessie.model.Reference;

/**
 * Contains custom Hamcrest {@link Matcher}s.
 * Might be handy in more complex scenarios compared to {@link org.hamcrest.Matchers#hasProperty(String, Matcher)}.
 */
public final class ReferenceMatchers {
  /**
   * Check whether {@link Reference#getName()} matches the given name.
   */
  public static Matcher<Reference> referenceWithName(@Nonnull String refName) {
    return new ReferenceByNameMatcher(refName, null);
  }

  /**
   * Check whether {@link Reference#getName()} matches the given name and the given reference type.
   */
  public static Matcher<Reference> referenceWithNameAndType(@Nonnull String refName, @Nonnull Class<? extends Reference> type) {
    return new ReferenceByNameMatcher(refName, type);
  }

  static class ReferenceByNameMatcher extends BaseMatcher<Reference> {

    private final String refName;
    private final Class<? extends Reference> type;

    ReferenceByNameMatcher(@Nonnull String refName, Class<? extends Reference> type) {
      this.refName = refName;
      this.type = type;
    }

    @Override
    public boolean matches(Object o) {
      if (!(o instanceof Reference)) {
        return false;
      }
      Reference ref = (Reference) o;
      return refName.equals(ref.getName())
             && (type == null || type.isInstance(ref));
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("reference with name ").appendText(refName);
      if (type != null) {
        description.appendText(" and type " + type.getSimpleName());
      }
    }

  }
}
