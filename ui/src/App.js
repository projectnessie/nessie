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
import {Navbar, Nav, Button, DropdownButton, Dropdown} from "react-bootstrap";
import {Router, Route} from 'react-router-dom';
import {history} from './utils';
import {authenticationService} from './services';
import {PrivateRoute} from './components';
import {HomePage} from './HomePage';
import {LoginPage} from './LoginPage';
import {config} from "./config";
import createApi from "./utils/api";
// import CreateBranchModal from './_old/services/CreateBranchModal';
// import MergeBranchModal from './_old/services/MergeBranchModal';

class App extends React.Component {
  constructor(props) {
    super(props);

    this.state = {
      currentUser: null,
      branches: [],
      currentBranch: "master",
      show: false,
      mergeShow: false,
      tables: []
    };
  }


  componentDidUpdate(prevProps, prevState, snapshot) {
    if (this.state.currentBranch !== prevState.currentBranch) {
      this.fetchTables();
    }
  }

  componentDidMount() {
    authenticationService.currentUser.subscribe(
      x => this.setState({currentUser: x}));
    this.fetchTables();
  }


  fetchTables() {

    if (this.state.currentUser && this.state.currentBranch) {
    const requestOptions = {
      method: 'GET',
      headers: {'Authorization': this.state.currentUser.token},
      tables: null,
      nestedTables: null
    };
    fetch(`${config.apiUrl}/objects/${this.state.currentBranch}/tables`, requestOptions)
      .then(res => {
        return res.json();
      })
      .then((data) => {
        this.setState({tables: data});
      })
      .catch(console.log);
    }
  }

  getBranches() {
    if (this.state.currentUser && this.state.branches.length === 0) {
      createApi({'cors':true}).getAllReferences()
      .then(res => {
        return res.json();
      })
      .then((data) => {
        this.setState({branches: data});
        // this.fetchTables()
      })
      .catch(console.log);
    }
  }

  logout() {
    authenticationService.logout();
    history.push('/login');
  }

  render() {
    this.getBranches();

    const handleClose = () => {
      this.setState({show: false, branches:[]});
      this.getBranches();
    }
    const handleShow = () => this.setState({show: true});
    const handleMergeClose = () => {
      this.setState({mergeShow: false});
      this.fetchTables();
    }
    const handleMergeShow = () => this.setState({mergeShow: true});
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
            </Nav>
          </Navbar>
          {/*<CreateBranchModal show={this.state.show} handleClose={handleClose} currentUser={this.state.currentUser} currentBranch={this.state.currentBranch}/>*/}
          {/*<MergeBranchModal show={this.state.mergeShow} handleClose={handleMergeClose} currentUser={this.state.currentUser} currentBranch={this.state.currentBranch}/>*/}

          <PrivateRoute exact path="/" component={HomePage} branches={this.state.branches} currentTables={this.state.tables}/>
          <Route path="/login" component={LoginPage} />
        </div>
      </Router>
    );
  }
}

export { App };
