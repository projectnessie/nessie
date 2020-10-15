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
import * as R from 'ramda';

export function nestTables(branch, tables) {
  let counter = 0;
  let nestedTables = {'name':branch};

  if (!tables.map) {
    return nestedTables;
  }
  let splitTables = tables.map(x => {
    let pieces = x.name.elements;
    const t = {'parts': pieces.slice(0, pieces.length-1),
      'name': pieces[pieces.length-1],
      'id': (counter++).toString()
    };
    t.namespace = t.parts.join('.');
    return t;
  });

  R.map((t) => {
    let x = nestedTables;
    t.parts.forEach(n => {
      if (!x.hasOwnProperty("children")) {
        x['children'] = [];
      }
      const existingNamespace = x.children.filter(t => t.name === n);
      if (existingNamespace.length === 0) {
        x.children.push({'name': n, 'id': (counter++).toString()});
      }
      x = x.children[x.children.length - 1];
    });
    if (!x.hasOwnProperty("children")) {
      x['children'] = [];
    }

    x.children.push(t);
    x.parts = t.parts;
    return t;
  }, splitTables);
  nestedTables['id'] = (counter++).toString();
  return nestedTables;
}
