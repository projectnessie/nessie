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
package org.projectnessie.operator.reconciler.nessie.dependent;

import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.projectnessie.operator.reconciler.nessie.NessieReconciler;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.reconciler.nessie.resource.options.ServiceAccountOptions;

@KubernetesDependent(labelSelector = NessieReconciler.DEPENDENT_RESOURCES_SELECTOR)
public class ServiceAccountDependent extends AbstractServiceAccountDependent<Nessie> {

  @Override
  public ServiceAccount desired(Nessie nessie, Context<Nessie> context) {
    return desired(nessie, nessie.getSpec().deployment().serviceAccount(), context);
  }

  public static class ActivationCondition
      extends AbstractServiceAccountDependent.ActivationCondition<Nessie> {

    @Override
    protected ServiceAccountOptions serviceAccount(Nessie primary) {
      return primary.getSpec().deployment().serviceAccount();
    }
  }
}
