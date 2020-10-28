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
