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
import {Card, CardGroup, Col, OverlayTrigger, Row, Table, Tooltip} from "react-bootstrap";
import CheckIcon from "@material-ui/icons/Check";
import Form from "react-bootstrap/Form";
import React, {useState} from "react";

function listToObj(l) {
  const obj = Object.assign({}, l)
  const ret = {};
  Object.keys(obj).forEach(key => {
    ret[obj[key].snapshotId] = key;
  });
  return ret;
}

export default function DisplaySnapshots(props) {
  const makeCard = (title, body) => {
    return (
      <Card style={{width: '18rem'}}>
        <Card.Header>{title}</Card.Header>
        <Card.Body>{body}</Card.Body>
      </Card>
    )
  }



  const snapshotIndex = props.snapshotIndex;
  const table = props.table;
  const snapshots = listToObj(table.snapshots);
  console.log(snapshots);
  const setSnapshotIndex = (x) => {
    props.setSnapshotIndex(x.target.value);
  }
  const cards = {
    'Snapshot Id': table.snapshots[snapshotIndex].snapshotId,
    'Parent Id': table.snapshots[snapshotIndex].parentId,
    'Change Time': new Date(table.snapshots[snapshotIndex].timestampMillis).toString(),
    'Operation': table.snapshots[snapshotIndex]["operation"],
    'Added Files': table.snapshots[snapshotIndex].summary['added-data-files'],
    'Added Records': table.snapshots[snapshotIndex].summary['added-records'],
    'Changed Partitions': table.snapshots[snapshotIndex].summary['changed-partition-count'],
    'Total Records': table.snapshots[snapshotIndex].summary['total-records'],
    'Total Files': table.snapshots[snapshotIndex].summary['total-data-files'],
  }
  const makePath = (path) => (
    <OverlayTrigger
      placement={'top'}
      overlay={
        <Tooltip id={path}>
          {path}
        </Tooltip>
      }
    >
      <span>{path.substring(0, 16) + "..."}</span>
    </OverlayTrigger>
  )
  const icon = (c) => c ? <CheckIcon/> : <span/>
  const flatten = (a) => (a == null) ? "" : JSON.stringify(Object.values(a))
  const makeRow = (row, color) => (
    <tr>
      <td>{icon(color)}</td>
      <td>{makePath(row.path)}</td>
      <td>{row.fileFormat}</td>
      <td>{row.recordCount}</td>
      <td>{row.fileSizeInBytes}</td>
      <td>{row.ordinal}</td>
      <td>{flatten(row.sortColumns)}</td>
      <td>{flatten(row.columnSizes)}</td>
      <td>{flatten(row.valueCounts)}</td>
      <td>{flatten(row.nullValueCounts)}</td>
    </tr>
  );
  const makeOption = (n) => (<option value={snapshots[n]}>{n}</option>)
  return (<div>
    <Row className={"py-2"}>
      <Col xs={8}/>
      <Col>
        <Form inline>
          <Form.Group controlId="exampleForm.SelectCustom">
            <Form.Label column={true}>Snapshot Id</Form.Label>
            <Form.Control as="select" onChange={setSnapshotIndex} custom>
              {table.snapshots.map(s => makeOption(s.snapshotId))}
            </Form.Control>
          </Form.Group>
        </Form></Col>
    </Row>
    <CardGroup>
      {Object.entries(cards).map(entry => makeCard(entry[0], entry[1]))}
    </CardGroup>
    <Table bordered hover responsive size={"sm"}>
      <thead>
      <tr>
        <th>added</th>
        <th>path</th>
        <th>fileFormat</th>
        <th>recordCount</th>
        <th>fileSizeInBytes</th>
        <th>ordinal</th>
        <th>sortColumns</th>
        <th>columnSizes</th>
        <th>valueCounts</th>
        <th>nullValueCounts</th>
      </tr>
      </thead>
      <tbody>
      {table.snapshots[snapshotIndex].addedFiles.map(x => makeRow(x, true))}
      {table.snapshots[snapshotIndex].deletedFiles.map(x => makeRow(x, false))}
      </tbody>
    </Table></div>)
}
