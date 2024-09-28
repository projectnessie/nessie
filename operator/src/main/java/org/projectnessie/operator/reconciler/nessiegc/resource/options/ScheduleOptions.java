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
package org.projectnessie.operator.reconciler.nessiegc.resource.options;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import io.fabric8.crd.generator.annotation.PrinterColumn;
import io.fabric8.generator.annotation.Default;
import io.fabric8.generator.annotation.Nullable;
import io.sundr.builder.annotations.Buildable;

@Buildable(builderPackage = "io.fabric8.kubernetes.api.builder", editableEnabled = false)
@JsonInclude(Include.NON_NULL)
public record ScheduleOptions(
    @JsonPropertyDescription(
            """
            The cron schedule to use; check https://crontab.guru for help with cron expressions. \
            By default, Nessie GC will run every day at 03:00 AM.""")
        @Default(DEFAULT_CRON)
        @PrinterColumn(name = "GC Schedule")
        String cron,
    @JsonPropertyDescription(
            """
            The time zone to use for the cron schedule. If not set, Nessie GC will rely on the \
            time zone of the kube-controller-manager process.""")
        @Nullable
        @jakarta.annotation.Nullable
        String timeZone,
    @JsonPropertyDescription("Whether to temporarily suspend the scheduling of GC jobs.")
        @Default("false")
        Boolean suspend) {

  public static final String DEFAULT_CRON = "0 3 * * *";

  public ScheduleOptions() {
    this(null, null, null);
  }

  public ScheduleOptions {
    cron = cron != null ? cron : DEFAULT_CRON;
    suspend = suspend != null ? suspend : false;
  }
}
