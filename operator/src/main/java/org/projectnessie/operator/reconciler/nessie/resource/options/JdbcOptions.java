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
package org.projectnessie.operator.reconciler.nessie.resource.options;

import static org.projectnessie.operator.events.EventReason.InvalidVersionStoreConfig;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import io.fabric8.generator.annotation.Nullable;
import io.fabric8.generator.annotation.Required;
import io.sundr.builder.annotations.Buildable;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.projectnessie.operator.exception.InvalidSpecException;

@Buildable(builderPackage = "io.fabric8.kubernetes.api.builder", editableEnabled = false)
@JsonInclude(Include.NON_NULL)
public record JdbcOptions(
    @JsonPropertyDescription("The JDBC connection URL (required).") //
        @Required
        String url,
    @JsonPropertyDescription("The JDBC username (optional).")
        @Nullable
        @jakarta.annotation.Nullable //
        String username,
    @JsonPropertyDescription("The JDBC password (optional).")
        @Nullable
        @jakarta.annotation.Nullable //
        SecretValue password) {

  private static final Pattern JDBC_URL_PATTERN = Pattern.compile("jdbc:(\\w+):.*");

  public record DataSource(String name) {

    public String configPrefix() {
      return "quarkus.datasource." + name() + ".";
    }
  }

  public DataSource datasource() {
    Matcher matcher = JDBC_URL_PATTERN.matcher(url());
    if (matcher.matches()) {
      return new DataSource(matcher.group(1).toLowerCase(Locale.ROOT));
    }
    throw new InvalidSpecException(InvalidVersionStoreConfig, "Invalid JDBC URL");
  }

  public void validate() {
    if (url() == null || url().isEmpty()) {
      throw new InvalidSpecException(InvalidVersionStoreConfig, "JDBC URL is required");
    }
    datasource();
  }
}
