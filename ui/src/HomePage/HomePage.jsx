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
import React, {useState} from 'react';

import {authenticationService} from '../services';
import RecursiveTreeView from "../TableSidebar/tree-view";
import TableView from "../TableSidebar/table-view";
import {Container, Row, Col} from "react-bootstrap";
import {config} from "../config";
import {nestTables} from "../utils";

function handleClick(event, table, tables, currentUser, currentBranch, setSelected ) {
  if (table < tables.length) {
    fetchTable(tables[table], currentUser, currentBranch, setSelected);
  }
}

function fetchTable(table, currentUser, currentBranch, setSelected) {
  if (currentUser && currentBranch) {
    const requestOptions = {
      method: 'GET',
      headers: {'Authorization': currentUser.token},
      tables: null,
      nestedTables: null
    };

    fetch(`${config.apiUrl}/objects/${currentBranch}/${table}?metadata=true`, requestOptions)
      .then(res => {
        return res.json();
      })
      .then((data) => {
        setSelected(data);
      })
      .catch(console.log);
  }
}

export default function HomePage(props) {
  const currentBranch = props.currentBranch;
  const currentUser = authenticationService.currentUserValue;
  const tables = props.currentTables;

  const [tableSelected, setSelected] = useState(null);
  // this.fetchTables();
  return (
    <div>
      <Container fluid>
        <Row>
          <Col xs={2}>
            <RecursiveTreeView tables={nestTables(currentBranch, tables)} onClick={(e,table) => handleClick(e, table, tables, currentUser, currentBranch, setSelected)}/>
          </Col>
          <Col>
            <TableView table={tableSelected}/>
          </Col>
        </Row>
      </Container>
    </div>
  );
}
