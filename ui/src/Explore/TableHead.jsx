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
import React, {Fragment} from 'react';
import PropTypes from 'prop-types';
import {Badge, Nav, NavDropdown} from "react-bootstrap";
import DeviceHubIcon from "@material-ui/icons/DeviceHub";
import LocalOfferIcon from "@material-ui/icons/LocalOffer";
import ExploreLink from "./ExploreLink";

function TableHead(props) {
  let additional;
  console.log("TableHead", props);
  if (props.path.length === 0) {
    additional = (
      <Fragment>
        <Nav.Item><Nav.Link><DeviceHubIcon/> {props.branches.length}</Nav.Link></Nav.Item>
        <Nav.Item><Nav.Link><LocalOfferIcon/> {props.tags.length}</Nav.Link></Nav.Item>
      </Fragment>
  );
  } else {
    additional = (
      <Nav.Item className="nav-link">
        <ExploreLink currentRef={props.currentRef}>nessie</ExploreLink>
        {
          props.path.map((p, index) => {
            return (<Fragment><span style={{"padding-left":7, "padding-right":7}}>/</span><ExploreLink currentRef={props.currentRef} path={props.path.slice(0,index+1)}>{p}</ExploreLink></Fragment>);
          })}
      </Nav.Item>
    );
  }

  return (
    <Nav variant={"pills"} activeKey={1}>
      <NavDropdown title={props.currentRef} id="nav-dropdown" style={{"padding-left":10}}>

        <NavDropdown.Item disabled={true}>Branches</NavDropdown.Item>
        {props.branches.map(branch => {
          return (<ExploreLink currentRef={branch.name} path={props.path} type="CONTAINER"><NavDropdown.Item as={"button"} key={branch.name}>
            {branch.name}
            <span>
              {(branch.name === props.defaultBranch) ? <Badge pill className="float-right" variant={"secondary"}>default</Badge> : ""}
          </span>
          </NavDropdown.Item></ExploreLink>)
        })}
        <NavDropdown.Divider/>
        <NavDropdown.Item disabled={true}>Tags</NavDropdown.Item>
        {props.tags.map(tag => {
          return (<NavDropdown.Item as={"button"} key={tag.name}><ExploreLink currentRef={tag} path={props.path} type="CONTAINER">{tag}</ExploreLink></NavDropdown.Item>)
        })}
        {/*<NavDropdown.Divider/>*/}
        {/*<NavDropdown.Item as={"button"} key={'Create Branch'}>Create Branch</NavDropdown.Item>*/}
      </NavDropdown>
      {additional}
    </Nav>
  )
}

TableHead.propTypes = {
  branches: PropTypes.arrayOf(PropTypes.shape({
    name: PropTypes.string.isRequired,
    hash: PropTypes.string.isRequired
  })).isRequired,
  tags: PropTypes.arrayOf(PropTypes.shape({
    name: PropTypes.string.isRequired,
    hash: PropTypes.string.isRequired
  })).isRequired,
  currentRef: PropTypes.string.isRequired,
  defaultBranch: PropTypes.string.isRequired
}

export default TableHead;
