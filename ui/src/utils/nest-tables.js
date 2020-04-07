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
import * as R from 'rambda';

export function nestTables(tables) {
  let counter = 0;
  counter += 1;
  let nestedTables = {'name':'root', 'id': counter.toString()};

  R.map((t) => {
    console.log(t);
    const parts = t.namespace.split('.');
    t.parts = parts;
    return t;
  }, tables.tables);
  R.map((t) => {
    let x = nestedTables;
    t.parts.forEach(n => {
      counter += 1;
      if (!x.hasOwnProperty("children")) {
        x['children'] = [];
      }
      x.children.push({'name':n, 'id':counter.toString()});
      x = x.children[x.children.length - 1];
    });
    if (!x.hasOwnProperty("children")) {
      x['children'] = [];
    }
    t['name'] = t['tableName'];
    counter += 1;
    t['id'] = counter.toString();
    x.children.push(t);
  }, tables.tables);
  console.log(nestedTables);
  console.log(tables);
  return nestedTables;
}
