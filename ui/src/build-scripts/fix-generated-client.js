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
console.log("Client API: Applying post-generation code fixups")

const replace = require('replace-in-file');

// openapi-generator produces Line 128 in runtime.ts as
//    export type FetchAPI = GlobalFetch['fetch'];
// but must be
//    export type FetchAPI = WindowOrWorkerGlobalScope['fetch'];
replace.sync({
  files: 'src/generated/utils/api/runtime.ts',
  from: /GlobalFetch/g,
  to: 'WindowOrWorkerGlobalScope',
})
