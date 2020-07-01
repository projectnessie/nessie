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
import './App.css';
import {Navbar, Nav, Button} from "react-bootstrap";
import {Router, Route} from 'react-router-dom';

import {history} from './utils';
import {authenticationService} from './services';
import {PrivateRoute} from './components';
import {HomePage} from './HomePage';
import {TagsPage} from './TagsPage';
import {AlertPage} from './AlertPage';
import {UsersPage} from './UsersPage';
import {LoginPage} from './LoginPage';

class App extends React.Component {
  constructor(props) {
    super(props);

    this.state = {
      currentUser: null
    };
  }

  componentDidMount() {
    authenticationService.currentUser.subscribe(x => this.setState({currentUser: x}));
  }

  logout() {
    authenticationService.logout();
    history.push('/login');
  }

  render() {
    // const {currentUser} = this.state;
    return (
      <Router history={history}>
        <div className="App">
          <Navbar bg="dark" expand="lg" fixed="top">
            <Navbar.Brand href="#home">
              <img
              alt=""
              src="/logo.svg"
              width="30"
              height="30"
              className="d-inline-block align-top"
            />{' '}Nessie</Navbar.Brand>
            <Nav className="mr-auto">
              <Nav.Link href="/">Tables</Nav.Link>
              <Nav.Link href="/tags">Branches</Nav.Link>
              <Nav.Link href="/alerts">Alerts</Nav.Link>
              <Nav.Link href="/users">Users</Nav.Link>
            </Nav>
            <Button bg="outline-dark" onClick={this.logout}>Logout</Button>
          </Navbar>
          {/*<Container fluid={true}>*/}
          {/*  <Row>*/}
          {/*    <Col>*/}
                <PrivateRoute exact path="/" component={HomePage} />
                <PrivateRoute path="/tags" component={TagsPage} />
                <PrivateRoute path="/alerts" component={AlertPage} />
                <PrivateRoute path="/users" component={UsersPage} />
                <Route path="/login" component={LoginPage} />
          {/*    </Col>*/}
          {/*  </Row>*/}
          {/*</Container>*/}
        </div>
      </Router>
    );
  }
}

export { App };
