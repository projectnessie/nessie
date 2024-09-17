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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.projectnessie.model.IdentifiedContentKey.IdentifiedElement.identifiedElement;
import static org.projectnessie.services.authz.ApiContext.apiContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IdentifiedContentKey;
import org.projectnessie.versioned.BranchName;

class TestRetriableAccessChecker {

  private int checkCount;
  private final List<Check> checked = new ArrayList<>();
  private final Map<Check, String> result = new HashMap<>();

  private final Supplier<BatchAccessChecker> validator =
      () ->
          new AbstractBatchAccessChecker(apiContext("Nessie", 1)) {
            @Override
            public Map<Check, String> check() {
              checkCount++;
              checked.clear();
              checked.addAll(getChecks());
              return result;
            }
          };

  @Test
  void checkAndThrow() {
    RetriableAccessChecker checker = new RetriableAccessChecker(validator, apiContext("Nessie", 1));
    Check check = Check.check(Check.CheckType.CREATE_ENTITY);
    result.put(check, "test123");
    assertThatThrownBy(() -> checker.newAttempt().can(check).checkAndThrow())
        .isInstanceOf(AccessCheckException.class)
        .hasMessage("test123");
    assertThat(checked).containsExactly(check);
    assertThat(checkCount).isEqualTo(1);
  }

  @Test
  void repeatedCheck() {
    RetriableAccessChecker checker = new RetriableAccessChecker(validator, apiContext("Nessie", 1));
    Check c1 = Check.check(Check.CheckType.CREATE_ENTITY);
    Check c2 = Check.check(Check.CheckType.CREATE_REFERENCE);
    assertThat(checker.newAttempt().can(c1).can(c2).check()).isEmpty();
    assertThat(checked).containsExactly(c1, c2);
    assertThat(checkCount).isEqualTo(1);

    // repeating the same checks in same order does not re-trigger validation
    assertThat(checker.newAttempt().can(c1).can(c2).check()).isEmpty();
    assertThat(checked).containsExactly(c1, c2);
    assertThat(checkCount).isEqualTo(1);

    // repeating the same checks in different order triggers re-validation
    assertThat(checker.newAttempt().can(c2).can(c1).check()).isEmpty();
    assertThat(checked).containsExactly(c2, c1);
    assertThat(checkCount).isEqualTo(2);
  }

  @Test
  void dataChangeBetweenAttempts() {
    IdentifiedContentKey.IdentifiedElement ns1 = identifiedElement("namespace", "id1");
    IdentifiedContentKey.IdentifiedElement ns2 = identifiedElement("namespace", "id2");
    IdentifiedContentKey.IdentifiedElement tableElement = identifiedElement("table", "id3");

    IdentifiedContentKey t1 =
        IdentifiedContentKey.builder()
            .type(Content.Type.ICEBERG_TABLE)
            .contentKey(ContentKey.of(ns1.element(), tableElement.element()))
            .addElements(ns1, tableElement)
            .build();

    // Note: only the namespace ID is different between `t1` and `t2`
    IdentifiedContentKey t2 =
        IdentifiedContentKey.builder()
            .type(Content.Type.ICEBERG_TABLE)
            .contentKey(ContentKey.of(ns2.element(), tableElement.element()))
            .addElements(ns2, tableElement)
            .build();

    RetriableAccessChecker checker = new RetriableAccessChecker(validator, apiContext("Nessie", 1));
    BranchName ref = BranchName.of("test");
    assertThat(checker.newAttempt().canCreateEntity(ref, t1).check()).isEmpty();
    assertThat(checked)
        .containsExactly(Check.canViewReference(ref), Check.canCreateEntity(ref, t1));
    assertThat(checkCount).isEqualTo(1);

    // repeating the attempt with different checks' data results in re-validation
    assertThat(checker.newAttempt().canCreateEntity(ref, t2).check()).isEmpty();
    assertThat(checked)
        .containsExactly(Check.canViewReference(ref), Check.canCreateEntity(ref, t2));
    assertThat(checkCount).isEqualTo(2);
  }
}
