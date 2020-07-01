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
import {Card, CardGroup} from "react-bootstrap";
import React from "react";
import * as R from "ramda";

function formatBytes(bytes, decimals = 2) {
  if (bytes === 0) return '0 Bytes';

  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];

  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

function totalSize(snapshots) {
  const added = R.sum(R.flatten(snapshots.map(s => s.addedFiles)).map(a => a.fileSyzeInBytes));
  const deleted = R.sum(R.flatten(snapshots.map(s => s.deletedFiles)).map(a => a.fileSyzeInBytes));
  return formatBytes(added+deleted);
}

const makeCard = (title, body) => {
  return (
    <Card style={{width: '18rem'}}>
      <Card.Header>{title}</Card.Header>
      <Card.Body>{body}</Card.Body>
    </Card>
  )
}

export default function DisplayMetadata(props) {
  console.log(props.table);
  const table = props.table;
  const snapshot = table.snapshots[table.snapshots.length-1];
  const cards = {
    'Table Id': table.uuid,
    'Source Id': table.sourceId,
    'Metadata Path': table.metadataLocation,
    'Current Id': snapshot.snapshotId,
    'Total Records': snapshot.summary['total-records'],
    'Total Files': snapshot.summary['total-data-files'],
    'Total Size': totalSize(table.snapshots)
  }
  return (<div>
    <CardGroup>
      {Object.entries(cards).map(entry => makeCard(entry[0], entry[1]))}
    </CardGroup>
  </div>)
}
