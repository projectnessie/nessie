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
package org.projectnessie.operator.reconciler.nessiegc.dependent;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import org.projectnessie.operator.events.EventReason;
import org.projectnessie.operator.events.EventService;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.reconciler.nessiegc.resource.NessieGc;

public class NessieDependent extends KubernetesDependentResource<Nessie, NessieGc> {

  public NessieDependent() {
    super(Nessie.class);
  }

  public static class ReconcilePrecondition implements Condition<Nessie, NessieGc> {

    @Override
    public boolean isMet(
        DependentResource<Nessie, NessieGc> dependentResource,
        NessieGc primary,
        Context<NessieGc> context) {
      boolean conditionMet = dependentResource.getSecondaryResource(primary, context).isPresent();
      if (!conditionMet) {
        EventService.retrieveFromContext(context)
            .fireEvent(
                primary,
                EventReason.NessieNotFound,
                String.format(
                    "Could not find Nessie %s in namespace %s",
                    primary.getSpec().nessieRef().getName(), primary.getMetadata().getNamespace()));
      }
      return conditionMet;
    }
  }
}
