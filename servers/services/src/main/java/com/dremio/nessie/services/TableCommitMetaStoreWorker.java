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
package com.dremio.nessie.services;

import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.Table;
import com.dremio.nessie.versioned.StoreWorker;

/**
 * Temporary class to implement serialization of Table and commitMeta.
 *
 * <p>
 *   Will be removed/moved when refactor to VersionStore is finished.
 * </p>
 */
public interface TableCommitMetaStoreWorker extends StoreWorker<Table, CommitMeta> {

}
