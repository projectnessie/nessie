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
import React, {useState, useEffect} from 'react';

import {authenticationService} from '../services';
import {Row, Card, Button, ListGroup, ListGroupItem} from "react-bootstrap";
import {nestTables} from "../utils";
import InsertDriveFileOutlinedIcon from '@material-ui/icons/InsertDriveFileOutlined';
import FolderIcon from '@material-ui/icons/Folder';
import createApi from "../utils/api"
import prettyMilliseconds from "pretty-ms";
import {ContentsModal} from "../ContentsModal";

function fetchLog(currentUser, currentBranch, setCurrentLog) {
  createApi({'cors':true}).getCommitLog({'ref': currentBranch.name})
    .then(res => {
      return res.json();
    })
    .then((data) => {
      setCurrentLog(data.operations);
    })
    .catch(console.log);
}

function fetchKeys(currentUser, currentBranch, setKeys) {
  createApi({'cors': true}).getEntries({'ref': currentBranch.name})
    .then(res => {
      return res.json();
    })
    .then((data) => {
      setKeys(nestTables(currentBranch.name, data.entries).children);
    })
    .catch(console.log);
}

function formatHeader(currentLog) {
  if (currentLog === undefined || currentLog.length === 0) {
    return (<Card.Header/>)
  }
  return (<Card.Header>
    <span className={"float-left"}>
      <span className="font-weight-bold">{currentLog[0].commiter}</span>
      <span>{currentLog[0].message}</span>
    </span>
    <span className={"float-right"}>
      <span className="font-italic">{currentLog[0].hash}</span>
      <span className={"pl-3"}>{prettyMilliseconds(new Date().getTime() - currentLog[0].commitTime, {compact:true})}</span>
    </span>
  </Card.Header>)
}

function formatItem(x, setPath, setContentsSelected) {
  if (x.children === undefined) {
    return (<span><InsertDriveFileOutlinedIcon/> <Button variant="link" onClick={() => {
      setContentsSelected(x);
    }}>{x.name}</Button></span>)
  } else {
    return (<span><FolderIcon/> <Button variant="link" onClick={() => setPath(x.parts)}>{x.name}</Button></span>)
  }
}

function formatList(keys, currentPath, setPath, setContentsSelected) {
  let currentKeys = keys;
  currentPath.forEach(x => {
    const filteredKeys = currentKeys.filter(y => y.name === x);
    if (filteredKeys.length > 0) {
      currentKeys = filteredKeys[0].children;
    }
  })
  return (
    <ListGroup variant={"flush"}>
      {currentKeys.map(x => {
        return (<ListGroupItem key={x.id}>{formatItem(x, setPath, setContentsSelected)}</ListGroupItem>)
      })}
    </ListGroup>
  )
}

function TableListing(props) {
  const currentBranch = props.currentBranch;
  const currentUser = authenticationService.currentUserValue;
  // const branches = props.branches;
  // const currentUser = authenticationService.currentUserValue;
  // const tables = props.currentTables;
  //
  // const [tableSelected, setSelected] = useState(null);
  // const [currentBranch, setCurrentBranch] = useState({name:"main"});
  const [contentsSelected, setContentsSelected] = useState({});
  const [contentsModalState, setContentsModalState] = useState(false);
  const [currentLog, setCurrentLog] = useState([]);
  const [keys, setKeys] = useState([]);
  const [currentPath, setPath] = useState([]);
  useEffect(() => {
    fetchLog(currentUser, currentBranch, setCurrentLog);
    fetchKeys(currentUser, currentBranch, setKeys)
  }, [currentBranch])

  return (
    <div>
      <Card>
        {formatHeader(currentLog)}
        {formatList(keys, currentPath, setPath, (x) => {
          setContentsSelected(x);
          setContentsModalState(true);
        })}
      </Card>
      <ContentsModal show={contentsModalState} handleClose={() => setContentsModalState(false)} currentUser={currentUser} currentBranch={currentBranch} currentKey={contentsSelected}/>
    </div>

  );
}

export { TableListing };
