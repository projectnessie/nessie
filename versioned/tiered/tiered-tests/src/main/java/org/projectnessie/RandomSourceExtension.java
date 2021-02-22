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
package org.projectnessie;

import java.util.Optional;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;

/**
 * A JUnit extension to setup the random generator seed before executing tests
 * and restoring it to its previous state after the test completes.
 *
 * <p>The seed value is specified using {@code WithSeed} annotation.
 */
public class RandomSourceExtension implements BeforeEachCallback, AfterEachCallback {
  private static final String RANDOM_CLOSEABLE = "Random Source Closeable";

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    // check for the seed annotation from the most specific context to the root
    ExtensionContext currentContext = context;
    while (currentContext != null) {
      // Look for a seed annotation
      Optional<Long> seed = currentContext.getElement()
          .map(a -> a.getAnnotation(WithSeed.class))
          .map(WithSeed::value);

      // If the current context is annotated with a seed,
      // set the random generator and exit
      if (seed.isPresent()) {
        Closeable c = RandomSource.withSeed(seed.get());
        getStore(context).put(RANDOM_CLOSEABLE, c);
        return;
      }

      currentContext = currentContext.getParent().orElse(null);
    }
  }


  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    Closeable c = (Closeable) getStore(context).get(RANDOM_CLOSEABLE);
    if (c != null) {
      c.close();
    }

  }

  private Store getStore(ExtensionContext context) {
    return context.getStore(Namespace.create(getClass(), context.getRequiredTestMethod()));
  }
}
