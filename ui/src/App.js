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
import React from "react";
import {Route, Switch} from "react-router-dom";
import Explore from "./Explore/Explore";
import {Nav, Navbar} from "react-bootstrap";


function App() {
  return (
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
        </Nav>

      </Navbar>
      <Switch>
        <Route path="/tree/:slug*" component={Explore} />
        <Route exact path="/" component={Explore} />
      </Switch>
    </div>
  );
}

export { App };
