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
package org.projectnessie.nessie.cli.commands;

import jakarta.annotation.Nonnull;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.projectnessie.client.api.OnReferenceBuilder;
import org.projectnessie.nessie.cli.cli.BaseNessieCli;
import org.projectnessie.nessie.cli.cmdspec.CommandSpec;
import org.projectnessie.nessie.cli.cmdspec.RefCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.RefWithHashCommandSpec;
import org.projectnessie.nessie.cli.grammar.Node;

public abstract class NessieCommand<SPEC extends CommandSpec> {
  protected NessieCommand() {}

  public abstract void execute(@Nonnull BaseNessieCli cli, SPEC spec) throws Exception;

  public abstract String name();

  public abstract String description();

  public abstract List<List<Node.NodeType>> matchesNodeTypes();

  public <B extends OnReferenceBuilder<B>> B applyReference(
      BaseNessieCli nessieCli, SPEC commandSpec, B onReferenceBuilder) {
    String refName = null;
    String hash = null;
    if (commandSpec instanceof RefCommandSpec) {
      refName = ((RefCommandSpec) commandSpec).getRef();
    }
    if (commandSpec instanceof RefWithHashCommandSpec) {
      hash = hashOrTimestamp((RefWithHashCommandSpec) commandSpec);
    }
    if (refName == null) {
      refName = nessieCli.getCurrentReference().getName();
    }

    return onReferenceBuilder.refName(refName).hashOnRef(hash);
  }

  protected String hashOrTimestamp(RefWithHashCommandSpec commandSpec) {
    String hash = commandSpec.getRefTimestampOrHash();
    if (hash == null) {
      return null;
    }
    try {
      Instant instant = ZonedDateTime.parse(hash).toInstant();
      long millis = ZonedDateTime.parse(hash).toInstant().toEpochMilli();
      // Silently add one millisecond if a fraction of a millisecond has been specified, because
      // we can only use millisecond precision via
      // org.projectnessie.client.api.OnReferenceBuilder.hashOnRef.
      if (instant.getNano() % TimeUnit.MILLISECONDS.toNanos(1) != 0) {
        millis++;
      }
      hash = "*" + millis;
    } catch (DateTimeParseException x) {
      // ignore
    }
    return hash;
  }
}
