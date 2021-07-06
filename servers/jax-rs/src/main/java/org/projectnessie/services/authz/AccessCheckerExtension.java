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
package org.projectnessie.services.authz;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.Extension;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.versioned.NamedRef;

/** This class needs to be in the same package as {@link AccessChecker}. */
public class AccessCheckerExtension implements Extension {
  public static final AccessChecker ACCESS_CHECKER =
      new AccessChecker() {
        @Override
        public void canViewReference(AccessContext context, NamedRef ref) {}

        @Override
        public void canCreateReference(AccessContext context, NamedRef ref) {}

        @Override
        public void canAssignRefToHash(AccessContext context, NamedRef ref) {}

        @Override
        public void canDeleteReference(AccessContext context, NamedRef ref) {}

        @Override
        public void canReadEntries(AccessContext context, NamedRef ref) {}

        @Override
        public void canListCommitLog(AccessContext context, NamedRef ref) {}

        @Override
        public void canCommitChangeAgainstReference(AccessContext context, NamedRef ref) {}

        @Override
        public void canReadEntityValue(
            AccessContext context, NamedRef ref, ContentsKey key, String contentsId) {}

        @Override
        public void canUpdateEntity(
            AccessContext context, NamedRef ref, ContentsKey key, String contentsId) {}

        @Override
        public void canDeleteEntity(
            AccessContext context, NamedRef ref, ContentsKey key, String contentsId) {}
      };

  @SuppressWarnings("unused")
  public void afterBeanDiscovery(@Observes AfterBeanDiscovery abd, BeanManager bm) {
    abd.addBean()
        .addType(AccessChecker.class)
        .addQualifier(Default.Literal.INSTANCE)
        .scope(ApplicationScoped.class)
        .produceWith(i -> ACCESS_CHECKER);
  }
}
