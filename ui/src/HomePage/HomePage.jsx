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
import {PrivateRoute} from "../components";
import {Route} from "react-router-dom";
import {LoginPage} from "../LoginPage";
import {Navbar, Nav, Button, Container, Row, Col} from "react-bootstrap";

class HomePage extends React.Component {
  constructor(props) {
    super(props);

    this.state = {
      currentUser: authenticationService.currentUserValue,
      users: null
    };
  }

  componentDidMount() {
    userService.getAll().then(users => this.setState({users}));
  }

  render() {
    const {currentUser, users} = this.state;
    return (
      <div>
        <Container fluid={true}>
          <Row>
            <Col md={2} className="sidebar">
              A
            </Col>
            <Col>
              B
            </Col>
          </Row>
        </Container>
      </div>
    );
  }
}

export {HomePage};
