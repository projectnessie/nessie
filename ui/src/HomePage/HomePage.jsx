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
import React from 'react';

import {userService, authenticationService} from '../services';
import RecursiveTreeView from "../TableSidebar/tree-view";
import TableView from "../TableSidebar/table-view";
import {Container, Row, Col} from "react-bootstrap";
import {config} from "../config";
import {nestTables} from "../utils";

class HomePage extends React.Component {
  constructor(props) {
    super(props);

    this.state = {
      currentUser: authenticationService.currentUserValue,
      users: null,
      selected: null
    };
    this.handleClick = this.handleClick.bind(this);
  }

  componentDidMount() {
    userService.getAll().then(users => this.setState({users}));
    const requestOptions = {
      method: 'GET',
      headers: {'Authorization': this.state.currentUser.token},
      tables: null,
      nestedTables: null
    };

    fetch(`${config.apiUrl}/api/v1/tables`, requestOptions)
      .then(res => {
        return res.json();
      })
      .then((data) => {
        this.setState({tables: data});
        return nestTables(data);
      })
      .then((data) => {
        this.setState({nestedTables: data})
      })
      .catch(console.log);
  }

  handleClick(event, table) {
    console.log(table);
    if (table < this.state.tables.length) {
      console.log(this.state.tables[table]);
      this.setState({selected: this.state.tables[table]});
    }
  }

  render() {
    const {nestedTables, selected} = this.state;
    return (
      <div>
        <Container fluid>
          <Row>
            <Col xs={2}>
              <RecursiveTreeView tables={nestedTables} onClick={this.handleClick}/>
            </Col>
            <Col>
                <TableView table={selected}/>
            </Col>
          </Row>
        </Container>
      </div>
    );
  }
}

export {HomePage};
