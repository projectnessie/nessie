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
import {
  Container,
  Nav,
  Navbar,
  Table
} from "react-bootstrap";
import CheckIcon from '@material-ui/icons/Check';
import DisplaySnapshots from "./snapshots";
import DisplayMetadata from "./metadata";


function displaySchema(table) {
  const schema = JSON.parse(table.metadata.schema);
  const getCheck = (required) => {
    if (required) {
      return (<CheckIcon/>)
    }
    return (<span/>)
  };
  let i = 0;
  const makeRow = (row) => (
    <tr key={i++}>
      <td>{row.id}</td>
      <td>{row.name}</td>
      <td>{row.type}</td>
      <td>{getCheck(row.required)}</td>
    </tr>
  );
  return (
    <Table striped bordered hover>
      <thead>
      <tr>
        <th>#</th>
        <th>Field Name</th>
        <th>Type</th>
        <th>Required</th>
      </tr>
      </thead>
      <tbody>
      {schema.fields.map(makeRow)}
      </tbody>
    </Table>
  )
}

export default function TableView(props) {
  const [table, setTable] = useState(props.table);
  const [active, setActive] = useState(1);
  const [snapshotIndex, setSnapshotIndex] = useState(0);
  useEffect(() => setTable(props.table), [props]);
  if (table == null || table.metadata == null) {
    return (<div/>)
  }

  return (
    <Container fluid className={"mainbar"}>
      <Navbar bg="light" expand="lg" className={"top-box"}>
        <Navbar.Brand className={"inner-brand"}>{table.namespace}</Navbar.Brand>
        <Navbar.Text className="mr-auto">
          {table.tableName}
        </Navbar.Text>
        <Navbar.Text>
          Snapshots: {table.metadata.snapshots.length} Last Updated: {new Date(table.updateTime).toString()}
        </Navbar.Text>
      </Navbar>
      <Nav fill variant="tabs" activeKey={active} onSelect={setActive} className={"middle-box"}>
        <Nav.Item>
          <Nav.Link eventKey={1}>Schema</Nav.Link>
        </Nav.Item>
        <Nav.Item>
          <Nav.Link eventKey={2}>Snapshots</Nav.Link>
        </Nav.Item>
        <Nav.Item>
          <Nav.Link eventKey={3}>Metadata</Nav.Link>
        </Nav.Item>
      </Nav>
      <div className={"bottom-box"}>
        <div>
          {/* eslint-disable-next-line eqeqeq */}
          <div style={(active == 1) ? {display: 'inline'}: {display:'none'}}> {displaySchema(table)} </div>
          {/* eslint-disable-next-line eqeqeq */}
          <div style={(active == 2) ? {display: 'inline'}: {display:'none'}}>
            <DisplaySnapshots table={table} snapshotIndex={snapshotIndex} setSnapshotIndex={setSnapshotIndex}/>
          </div>
          {/* eslint-disable-next-line eqeqeq */}
          <div style={(active == 3) ? {display: 'inline'}: {display:'none'}}>
            <DisplayMetadata table={table}/></div>
          </div>


      </div>
    </Container>

  )


}



