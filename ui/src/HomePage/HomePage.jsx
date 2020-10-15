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

import {authenticationService} from '../services';
import {Container, NavDropdown, Nav, Badge} from "react-bootstrap";
import {config} from "../config";
import DeviceHubIcon from '@material-ui/icons/DeviceHub';
import LocalOfferIcon from '@material-ui/icons/LocalOffer';
import {TableListing} from "../TableListing";
import createApi from "../utils/api"


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

function fetchDefaultBranch(currentUser, setBranch, setDefaultBranch) {
  if (currentUser) {
    createApi({'cors':true}).getDefaultBranch()
      .then(res => {
        return res.json();
      })
      .then((data) => {
        setBranch(data);
        setDefaultBranch(data);
      })
      .catch(console.log);
  }
}

function header(currentBranch) {
  return (<span>{(currentBranch.type === "BRANCH") ? <DeviceHubIcon/> : <LocalOfferIcon/>} {currentBranch.name}</span>)
}

function item(currentBranch, defaultBranch) {
  return (<span>{currentBranch.name} {(currentBranch.name === defaultBranch.name) ? <Badge pill className="float-right" variant={"secondary"}>default</Badge> : ""}</span>)
}
function HomePage(props) {
  const branches = props.branches;
  const currentUser = authenticationService.currentUserValue;

  const [currentBranch, setCurrentBranch] = useState({name:"main"});
  const [defaultBranch, setDefaultBranch] = useState({name:"main"});
  useEffect(() => {fetchDefaultBranch(currentUser, setCurrentBranch, setDefaultBranch)}, [])

  return (
    <div>
      <Container style={{"marginTop": "100px"}}>
        <Nav variant={"pills"} activeKey={1}>
          <NavDropdown title={header(currentBranch)} id="nav-dropdown">
            <NavDropdown.Item disabled={true}>Branches</NavDropdown.Item>
            {branches.filter(x => x.type === "BRANCH").map(x => {
              return (<NavDropdown.Item as={"button"} key={x.name} onClick={y=>{
                setCurrentBranch(x);
              }}>{item(x, defaultBranch)}</NavDropdown.Item>)
            })}
            <NavDropdown.Divider />
            <NavDropdown.Item disabled={true}>Tags</NavDropdown.Item>
            {branches.filter(x => x.type === "TAG").map(x => {
              return (<NavDropdown.Item as={"button"} key={x.name} onClick={y=>{
                setCurrentBranch(x);
              }}>{x.name}</NavDropdown.Item>)
            })}
            <NavDropdown.Divider />
            <NavDropdown.Item as={"button"} key={'Create Branch'} >Create Branch</NavDropdown.Item>
          </NavDropdown>
          <Nav.Item>
            <Nav.Link>
              <DeviceHubIcon/> {branches.filter(x => x.type === "BRANCH").length}
            </Nav.Link>
          </Nav.Item>
          <Nav.Item>
            <Nav.Link>
              <LocalOfferIcon/> {branches.filter(x => x.type === "TAG").length}
            </Nav.Link>
          </Nav.Item>
        </Nav>
        <TableListing currentBranch={currentBranch}/>
      </Container>
    </div>
  );
}

export { HomePage };
