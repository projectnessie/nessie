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
import {Navbar, Nav, Button, Container, Row, Col} from "react-bootstrap";
import {Router, Route, Link} from 'react-router-dom';

import {history} from './utils';
import {authenticationService} from './services';
import {PrivateRoute} from './components';
import {HomePage} from './HomePage';
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
    const {currentUser} = this.state;
    return (
      <Router history={history}>
        <div className="App">
          <Navbar bg="dark" expand="lg" fixed="top">
            <Navbar.Brand href="#home">Iceberg Alley</Navbar.Brand>
            <Nav className="mr-auto">
              {/*todo link these correctly*/}
              <Nav.Link href="#tables">Tables</Nav.Link>
              <Nav.Link href="#tags">Tags</Nav.Link>
              <Nav.Link href="#alerts">Alerts</Nav.Link>
            </Nav>
            {/*todo show login when logged in and logout when logged out*/}
            <Button bg="outline-dark" onClick={this.logout}>Logout</Button>
          </Navbar>
          <Container fluid={true}>
            <Row>
              <Col>
                <PrivateRoute exact path="/" component={HomePage} />
                <Route path="/login" component={LoginPage} />
              </Col>
            </Row>
          </Container>
        </div>
      </Router>
    );
  }
}

export { App };
