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

import java.security.AccessControlException;
import java.util.function.Supplier;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.Extension;
import org.projectnessie.model.ContentKey;
import org.projectnessie.versioned.NamedRef;

/** This class needs to be in the same package as {@link AccessChecker}. */
public class AccessCheckerExtension implements Extension {
  private static final AccessChecker NOOP_ACCESS_CHECKER = new DefaultAccessChecker();

  private volatile Supplier<AccessChecker> accessCheckerSupplier;

  private AccessChecker accessCheckerInstance() {
    Supplier<AccessChecker> supplier = accessCheckerSupplier;
    AccessChecker instance = supplier != null ? supplier.get() : null;
    return instance != null ? instance : NOOP_ACCESS_CHECKER;
  }

  private final AccessChecker delegator =
      new AccessChecker() {
        @Override
        public void canViewReference(AccessContext context, NamedRef ref)
            throws AccessControlException {
          accessCheckerInstance().canViewReference(context, ref);
        }

        @Override
        public void canCreateReference(AccessContext context, NamedRef ref)
            throws AccessControlException {
          accessCheckerInstance().canCreateReference(context, ref);
        }

        @Override
        public void canAssignRefToHash(AccessContext context, NamedRef ref)
            throws AccessControlException {
          accessCheckerInstance().canAssignRefToHash(context, ref);
        }

        @Override
        public void canDeleteReference(AccessContext context, NamedRef ref)
            throws AccessControlException {
          accessCheckerInstance().canDeleteReference(context, ref);
        }

        @Override
        public void canReadEntries(AccessContext context, NamedRef ref)
            throws AccessControlException {
          accessCheckerInstance().canReadEntries(context, ref);
        }

        @Override
        public void canListCommitLog(AccessContext context, NamedRef ref)
            throws AccessControlException {
          accessCheckerInstance().canListCommitLog(context, ref);
        }

        @Override
        public void canCommitChangeAgainstReference(AccessContext context, NamedRef ref)
            throws AccessControlException {
          accessCheckerInstance().canCommitChangeAgainstReference(context, ref);
        }

        @Override
        public void canReadEntityValue(
            AccessContext context, NamedRef ref, ContentKey key, String contentId)
            throws AccessControlException {
          accessCheckerInstance().canReadEntityValue(context, ref, key, contentId);
        }

        @Override
        public void canUpdateEntity(
            AccessContext context, NamedRef ref, ContentKey key, String contentId)
            throws AccessControlException {
          accessCheckerInstance().canUpdateEntity(context, ref, key, contentId);
        }

        @Override
        public void canDeleteEntity(
            AccessContext context, NamedRef ref, ContentKey key, String contentId)
            throws AccessControlException {
          accessCheckerInstance().canDeleteEntity(context, ref, key, contentId);
        }

        @Override
        public void canViewRefLog(AccessContext context) throws AccessControlException {
          accessCheckerInstance().canViewRefLog(context);
        }
      };

  public AccessCheckerExtension setAccessCheckerSupplier(
      Supplier<AccessChecker> accessCheckerSupplier) {
    this.accessCheckerSupplier = accessCheckerSupplier;
    return this;
  }

  @SuppressWarnings("unused")
  public void afterBeanDiscovery(@Observes AfterBeanDiscovery abd, BeanManager bm) {
    abd.addBean()
        .addType(AccessChecker.class)
        .addQualifier(Default.Literal.INSTANCE)
        .scope(ApplicationScoped.class)
        .produceWith(i -> delegator);
  }
}
