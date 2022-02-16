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
package org.projectnessie.jaxrs;

/**
 * Base test class for Nessie REST APIs.
 *
 * <h2>Base class organization</h2>
 *
 * (See below for reasons)
 *
 * <p>This abstract test class represents the base test class for Jersey based tests and tests in
 * Quarkus.
 *
 * <p>Every base class must extend each other, as a rule of thumb, maintain alphabetical order.
 *
 * <p>Another rule of thumb: when a class reaches ~ 500 LoC, split it, when it makes sense.
 *
 * <h2>Reasons for the "chain of base classes"</h2>
 *
 * <p>Tests have been moved to base classes so that this class is not a "monster of couple thousand
 * lines of code".
 *
 * <p>Splitting {@code AbstractTestRest} into separate classes and including those via
 * {@code @Nested} with plain JUnit 5 works fine. But it does not work as a {@code @QuarkusTest} for
 * several reasons.
 *
 * <p>Using {@code @Nested} test classes like this does not work, because Quarkus considers the
 * non-static inner classes as a separate test classes and fails horribly, mostly complaining that
 * the inner test class is not in one of the expected class folders.
 *
 * <p>Even worse is that the whole Quarkus machinery is started for each nested test class and not
 * just for the actual test class.
 *
 * <p>As an alternative it felt doable to refactor the former inner classes to interfaces (so test
 * methods become default methods), but that does not work as well due to an NPE, when {@code
 * QuarkusSecurityTestExtension.beforeEach} tries to find the {@code @TestSecurity} annotation - it
 * just asserts whether the current {@code Class} is {@code != Object.class}, but {@code
 * Class.getSuperclass()} returns {@code null} for interfaces - hence the NPE there.
 *
 * <p>The "solution" here is to keep the separate classes but let each extend another - so different
 * groups of tests are kept in a single class.
 */
public abstract class AbstractTestRest extends AbstractRestRefLog {
  // Entry class for the test hierarchy
}
