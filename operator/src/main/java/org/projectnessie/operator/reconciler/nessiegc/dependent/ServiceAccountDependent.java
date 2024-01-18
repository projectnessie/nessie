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

import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.projectnessie.operator.reconciler.nessie.dependent.AbstractServiceAccountDependent;
import org.projectnessie.operator.reconciler.nessie.resource.options.ServiceAccountOptions;
import org.projectnessie.operator.reconciler.nessiegc.NessieGcReconciler;
import org.projectnessie.operator.reconciler.nessiegc.resource.NessieGc;

@KubernetesDependent(labelSelector = NessieGcReconciler.DEPENDENT_RESOURCES_SELECTOR)
public class ServiceAccountDependent extends AbstractServiceAccountDependent<NessieGc> {

  @Override
  public ServiceAccount desired(NessieGc nessie, Context<NessieGc> context) {
    return desired(nessie, nessie.getSpec().job().serviceAccount(), context);
  }

  public static class ActivationCondition
      extends AbstractServiceAccountDependent.ActivationCondition<NessieGc> {

    @Override
    protected ServiceAccountOptions serviceAccount(NessieGc primary) {
      return primary.getSpec().job().serviceAccount();
    }
  }
}
