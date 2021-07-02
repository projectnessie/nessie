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
import React, {useEffect, useState} from 'react';
import {Card, ListGroup, ListGroupItem} from "react-bootstrap";
import InsertDriveFileOutlinedIcon from '@material-ui/icons/InsertDriveFileOutlined';
import FolderIcon from '@material-ui/icons/Folder';
import ExploreLink from "./ExploreLink";
import {api} from "../utils";
import {Entry} from "../utils";


function groupItem(key: Key, ref: string, path: Array<string>) {
  let icon = key.type === "CONTAINER" ? <FolderIcon/> : <InsertDriveFileOutlinedIcon/>;
  return (
    <ListGroupItem key={key.name}>
      <ExploreLink toRef={ref} path={path.concat(key.name)} type={key.type === "CONTAINER" ? "CONTAINER" : "OBJECT"}>
        {icon}{key.name}
      </ExploreLink>
    </ListGroupItem>
  )
}

function fetchKeys(ref: string, path: Array<string>, setKeys: (v: Array<Key>) => void) {
  return api().getEntries({'ref':ref})
    .then((data) => {
      let keys = filterKeys(path, data.entries ?? []);
      setKeys(keys);
    }).catch(e => console.log(e));
}

interface Key {
 name: string;
 type: "CONTAINER" | "TABLE";
}

// TODO: move this to server-side. Filter to keys relevant for this view.
function filterKeys(path: Array<string>, keys: Array<Entry>) : Array<Key> {
  let containers = new Set();
  let filteredKeys = keys
    .map(key => key.name?.elements)
    .filter(name => {
      if (!name || name.length <= path.length) {
        return false;
      }

      // make sure all key values match the current path.
      return name.slice(0, path.length).every((v, i) => v.toLowerCase() === path[i].toLowerCase());
    })
    .map(s => s as Array<string>)
    .map((name) => {
      let ele = name[path.length];
      if (name.length > path.length + 1) {
        containers.add(ele);
      }
      return ele;
    });

  let distinctKeys = new Set(filteredKeys);
  let distinctObjs = Array.from(distinctKeys).map(key => {
    return {
      name: key,
      type: containers.has(key) ? "CONTAINER" : "TABLE"
    } as Key;
  })
  return distinctObjs;
}

function TableListing(props: {
  currentRef: string,
  path: Array<string>
}) {
  const [keys, setKeys] = useState<Array<Key>>([]);
  useEffect(() => {
    fetchKeys(props.currentRef, props.path, setKeys);
  }, [props.currentRef, props.path])

  return (
    <Card>
      <ListGroup variant={"flush"}>
        {keys.map((key) => {return groupItem(key, props.currentRef, props.path);})}
      </ListGroup>
    </Card>
  );
}

TableListing.defaultProps = {
  currentRef: "main",
  path: [],
}

export default TableListing;
