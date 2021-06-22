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

import {nessieMinApiVersion} from "./version-numbers";

export const minServerVersion = parseVersion(nessieMinApiVersion);

export function verifyServerVersion(headers: Headers) {
  const serverVersion = headers.get("Nessie-Version")
  if (serverVersion == null) {
    if (headers.get("Content-Type") == null) {
      // Ignore missing 'Nessie-Version' header when there's no content, because those
      // responses don't get the Nessie-Version header (REST API restriction/inconvenience).
      return null;
    }
    return Promise.reject(`Nessie-Server sent no Nessie-Version header`)
  }
  if (compareArray(minServerVersion, parseVersion(serverVersion)) > 0) {
    return Promise.reject(`Nessie-Server version ${serverVersion} is incompatible, minimum required is ${minServerVersion}`)
  }
  return null;
}

function compareArray(arr1: Array<number>, arr2: Array<number>) {
  let i = 0;
  console.log(`compare ${arr1} vs ${arr2}`)
  for (; i < arr1.length && i < arr2.length; i++) {
    const el1 = arr1[i]
    const el2 = arr2[i]
    console.log(`  ${i} -> compare ${el1} vs ${el2}`)
    if (el1 > el2) {
      console.log(`  gt`)
      return 1;
    }
    if (el1 < el2) {
      console.log(`  lt`)
      return -1;
    }
  }
  if (i < arr1.length) {
    console.log(`  gt e`)
    return 1;
  }
  if (i < arr2.length) {
    console.log(`  lt e`)
    return -1;
  }
  console.log(`  eq`)
  return 0;
}

function parseVersion(str: string) {
  return str.replace(/(.*)-SNAPSHOT/, "$1").split(".").map(v => parseInt(v));
}

