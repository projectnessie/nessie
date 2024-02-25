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
package org.projectnessie.operator.events;

public enum EventReason {

  // Normal events
  CreatingServiceAccount(EventType.Normal),
  CreatingConfigMap(EventType.Normal),
  CreatingPersistentVolumeClaim(EventType.Normal),
  CreatingDeployment(EventType.Normal),
  CreatingService(EventType.Normal),
  CreatingMgmtService(EventType.Normal),
  CreatingServiceMonitor(EventType.Normal),
  CreatingIngress(EventType.Normal),
  CreatingHPA(EventType.Normal),
  ReconcileSuccess(EventType.Normal),

  // Warning events
  InvalidName(EventType.Warning),
  InvalidAuthenticationConfig(EventType.Warning),
  InvalidAuthorizationConfig(EventType.Warning),
  InvalidTelemetryConfig(EventType.Warning),
  InvalidAutoScalingConfig(EventType.Warning),
  InvalidIngressConfig(EventType.Warning),
  InvalidVersionStoreConfig(EventType.Warning),
  InvalidAdvancedConfig(EventType.Warning),
  DuplicateEnvVar(EventType.Warning),
  MultipleReplicasNotAllowed(EventType.Warning),
  AutoscalingNotAllowed(EventType.Warning),
  ServiceMonitorNotSupported(EventType.Warning),
  ReconcileError(EventType.Warning),
  ;

  private final EventType type;

  EventReason(EventType type) {
    this.type = type;
  }

  public EventType type() {
    return type;
  }
}
