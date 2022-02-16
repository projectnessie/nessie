/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.persist.tests;

import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterConfigItem;

@NessieDbAdapterConfigItem(name = "retry.initial.sleep.millis.lower", value = "25")
@NessieDbAdapterConfigItem(name = "retry.initial.sleep.millis.upper", value = "50")
@NessieDbAdapterConfigItem(name = "retry.max.sleep.millis", value = "150")
@NessieDbAdapterConfigItem(name = "commit.timeout", value = "500")
public interface LongerCommitTimeouts {}
