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
package org.projectnessie.services.authz;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A utility class for performing access check in contexts where operations may have to be re-tried
 * due to optimistic locking or similar mechanisms. Each {@link #newAttempt() attempt} forms a new
 * batch of access checks, but they are not re-validated on subsequent attempts unless the new batch
 * of checks is different from the already validated one. The order of checks matters.
 */
public final class RetriableAccessChecker {
  private final Supplier<BatchAccessChecker> validator;
  private final ApiContext apiContext;
  private Collection<Check> validatedChecks;
  private Map<Check, String> result;

  public RetriableAccessChecker(Supplier<BatchAccessChecker> validator, ApiContext apiContext) {
    Preconditions.checkNotNull(validator);
    this.validator = validator;
    this.apiContext = apiContext;
  }

  public BatchAccessChecker newAttempt() {
    return new Attempt(apiContext);
  }

  private class Attempt extends AbstractBatchAccessChecker {
    private final ApiContext apiContext;

    Attempt(ApiContext apiContext) {
      super(apiContext);
      this.apiContext = apiContext;
    }

    @Override
    public ApiContext getApiContext() {
      return apiContext;
    }

    @Override
    public Map<Check, String> check() {
      // Shallow collection copy to ensure that we use what was current at the time of check
      // in the equals call below (in case checks are added to this instance later, for whatever
      // reason). Note that elements are immutable.
      Collection<Check> currentChecks = new ArrayList<>(getChecks());

      if (validatedChecks != null && result != null && validatedChecks.equals(currentChecks)) {
        return result;
      }

      BatchAccessChecker checker = validator.get();
      currentChecks.forEach(checker::can);
      result = checker.check();
      validatedChecks = currentChecks;
      return result;
    }
  }
}
