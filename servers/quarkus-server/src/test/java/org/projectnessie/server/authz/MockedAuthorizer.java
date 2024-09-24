/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.server.authz;

import static org.projectnessie.server.authz.MockedAuthorizer.AuthzCheck.authzCheck;
import static org.projectnessie.services.authz.Check.check;

import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import org.projectnessie.nessie.immutables.NessieImmutable;
import org.projectnessie.services.authz.AbstractBatchAccessChecker;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.ApiContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.authz.AuthorizerType;
import org.projectnessie.services.authz.BatchAccessChecker;
import org.projectnessie.services.authz.Check;

@AuthorizerType("MOCKED")
@Singleton
public class MockedAuthorizer implements Authorizer {
  private BiFunction<BatchAccessChecker, Collection<Check>, Map<Check, String>> responder =
      (b, c) -> Map.of();
  private final List<AuthzCheck> checks = new ArrayList<>();

  @Override
  public BatchAccessChecker startAccessCheck(AccessContext context, ApiContext apiContext) {
    return new MockedBatchAccessChecker(context, apiContext);
  }

  public synchronized void setResponder(
      BiFunction<BatchAccessChecker, Collection<Check>, Map<Check, String>> responder) {
    this.responder = responder;
  }

  public synchronized void reset() {
    checks.clear();
    responder = (b, c) -> Map.of();
  }

  public synchronized List<AuthzCheck> checks() {
    return List.copyOf(checks);
  }

  public List<AuthzCheck> checksWithoutIdentifiedKey() {
    return checks().stream()
        .map(
            ac ->
                authzCheck(
                    ac.apiContext(),
                    ac.checks().stream()
                        .map(c -> check(c.type(), c.ref(), c.key(), c.actions()))
                        .toList(),
                    ac.response()))
        .toList();
  }

  synchronized void addCheck(AuthzCheck authzCheck) {
    checks.add(authzCheck);
  }

  public class MockedBatchAccessChecker extends AbstractBatchAccessChecker {
    public final AccessContext context;

    public MockedBatchAccessChecker(AccessContext context, ApiContext apiContext) {
      super(apiContext);
      this.context = context;
    }

    @Override
    public Map<Check, String> check() {
      var response = responder.apply(this, this.getChecks());
      addCheck(authzCheck(getApiContext(), getChecks(), response));
      return response;
    }
  }

  @NessieImmutable
  public interface AuthzCheck {
    ApiContext apiContext();

    Set<Check> checks();

    Map<Check, String> response();

    static AuthzCheck authzCheck(
        ApiContext apiContext, Collection<Check> checks, Map<Check, String> response) {
      return ImmutableAuthzCheck.of(apiContext, new LinkedHashSet<>(checks), response);
    }
  }
}
