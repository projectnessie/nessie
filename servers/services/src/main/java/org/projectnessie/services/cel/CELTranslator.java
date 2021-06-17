/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.services.cel;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import org.projectnessie.api.params.CommitLogParams;
import org.projectnessie.api.params.EntriesParams;

/**
 * The purpose of the {@link CELTranslator} is to translate given filtering parameters, such as
 * {@link CommitLogParams} / {@link EntriesParams} into valid CEL expressions as defined in the <a
 * href="https://github.com/google/cel-spec/blob/master/doc/langdef.md">CEL language definition</a>.
 *
 * @see <a href="https://github.com/google/cel-spec/blob/master/doc/langdef.md">CEL language
 *     definition</a>
 */
public class CELTranslator {

  public static String from(CommitLogParams params) {
    Preconditions.checkArgument(null != params, "Commit log filtering params must be non-null");

    String authorExpr =
        or(
            params.getAuthors().stream()
                .map(x -> String.format("commit.author=='%s'", x))
                .collect(Collectors.toList()));
    authorExpr = params.getAuthors().size() > 1 ? wrap(authorExpr) : authorExpr;

    String committerExpr =
        or(
            params.getCommitters().stream()
                .map(c -> String.format("commit.committer=='%s'", c))
                .collect(Collectors.toList()));
    committerExpr = params.getCommitters().size() > 1 ? wrap(committerExpr) : committerExpr;

    StringJoiner and = new StringJoiner(" && ");
    if (!Strings.isNullOrEmpty(authorExpr)) {
      and.add(authorExpr);
    }
    if (!Strings.isNullOrEmpty(committerExpr)) {
      and.add(committerExpr);
    }

    if (null != params.getAfter()) {
      and.add(String.format("timestamp(commit.commitTime) > timestamp('%s')", params.getAfter()));
    }
    if (null != params.getBefore()) {
      and.add(String.format("timestamp(commit.commitTime) < timestamp('%s')", params.getBefore()));
    }
    return and.toString();
  }

  public static String from(EntriesParams params) {
    Preconditions.checkArgument(null != params, "Entries filtering params must be non-null");
    return "";
  }

  private static String or(List<String> items) {
    StringJoiner or = new StringJoiner(" || ");
    items.forEach(or::add);
    return or.toString();
  }

  private static String and(List<String> items) {
    StringJoiner or = new StringJoiner(" && ");
    items.forEach(or::add);
    return or.toString();
  }

  private static String wrap(String s) {
    return "(" + s + ")";
  }
}
