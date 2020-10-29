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
import PropTypes from 'prop-types';
import {api} from "../utils";


function groupItem(name, type, ref, path) {
  let icon = type === "CONTAINER" ? <FolderIcon/> : <InsertDriveFileOutlinedIcon/>;
  return (
    <ListGroupItem key={name}>
      <ExploreLink toRef={ref} path={path.concat(name)} type={type === "CONTAINER" ? "CONTAINER" : "OBJECT"}>
        {icon}{name}
      </ExploreLink>
    </ListGroupItem>
  )
}

function fetchKeys(ref, path, setKeys) {
  return api().getEntries({'ref':ref})
    .then(res => {
      return res.json();
    })
    .then((data) => {
      let keys = filterKeys(path, data.entries);
      setKeys(keys);
    }).catch(e => console.log(e));
}

// TODO: move this to server-side. Filter to keys relevant for this view.
function filterKeys(path, keys) {
  let containers = new Set();
  let filteredKeys = keys.map(key => key.name.elements)
    .filter(name => {
      if (name.length <= path.length) {
        return false;
      }

      // make sure all key values match the current path.
      return name.slice(0, path.length).every((v, i) => v.toLowerCase() === path[i].toLowerCase());
    })
    .map(name => {
      let ele = name[path.length];
      if (name.length > path.length + 1) {
        containers.add(ele);
      }
      return ele;
    });

  let distinctKeys = [...new Set(filteredKeys)];
  let distinctObjs = distinctKeys.map(key => {
    return {
      name: key,
      type: containers.has(key) ? "CONTAINER" : "TABLE"
    };
  })
  return distinctObjs;
}

function TableListing(props) {
  const [keys, setKeys] = useState([]);
  useEffect(() => {
    fetchKeys(props.currentRef, props.path, setKeys);
  }, [props.currentRef, props.path])

  return (
    <Card>
      <ListGroup variant={"flush"}>
        {keys.map(key => {return groupItem(key.name, key.type, props.currentRef, props.path);})}
      </ListGroup>
    </Card>
  );
}

TableListing.propTypes = {
  currentRef: PropTypes.string.isRequired,
  path: PropTypes.arrayOf(PropTypes.string).isRequired,
  currentLog: PropTypes.shape({
    committer: PropTypes.string.isRequired,
    hash: PropTypes.string.isRequired,
    message: PropTypes.string.isRequired,
    commitTime: PropTypes.number.isRequired
  })
}

TableListing.defaultProps = {
  currentRef: "main",
  path: [],
}

export default TableListing;
