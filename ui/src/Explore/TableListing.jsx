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
import React, {Fragment} from 'react';
import {Card, ListGroup, ListGroupItem} from "react-bootstrap";
import InsertDriveFileOutlinedIcon from '@material-ui/icons/InsertDriveFileOutlined';
import FolderIcon from '@material-ui/icons/Folder';
import prettyMilliseconds from "pretty-ms";
import ExploreLink from "./ExploreLink";

function formatHeader(currentLog) {
  if (currentLog === undefined) {
    return (<Card.Header/>)
  }
  return (<Card.Header>
    <span className={"float-left"}>
      <span className="font-weight-bold">{currentLog.commiter}</span>
      <span>{currentLog.message}</span>
    </span>
    <span className={"float-right"}>
      <span className="font-italic">{currentLog.hash.slice(0,8)}</span>
      <span
        className={"pl-3"}>{prettyMilliseconds(new Date().getTime() - currentLog.commitTime, {compact: true})}</span>
    </span>
  </Card.Header>)
}

function groupItem(name, type, ref, path) {
  let icon = type === "CONTAINER" ? <FolderIcon/> : <InsertDriveFileOutlinedIcon/>;
  return (
    <ListGroupItem key={name}>
      {/*{item}*/}
      <ExploreLink currentRef={ref} path={path.concat(name)} type={type === "CONTAINER" ? "CONTAINER" : "OBJECT"}>
        {icon}{name}
      </ExploreLink>
    </ListGroupItem>
  )
}

function TableListing(props) {
  return (
    <Fragment>
      {formatHeader(props.currentLog)}
      <Card>
        <ListGroup variant={"flush"}>
          {props.keys.map(key => {return groupItem(key.name, key.type, props.currentRef, props.path);})}
        </ListGroup>
      </Card>
      {/*<ContentsModal show={contentsModalState} handleClose={() => setContentsModalState(false)}*/}
      {/*               currentUser={currentUser} currentBranch={currentBranch} currentKey={contentsSelected}/>*/}
    </Fragment>

  );
}

export default TableListing;
