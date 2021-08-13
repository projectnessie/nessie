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
import {Badge, Nav, NavDropdown} from "react-bootstrap";
import DeviceHubIcon from "@material-ui/icons/DeviceHub";
import LocalOfferIcon from "@material-ui/icons/LocalOffer";
import ExploreLink from "./ExploreLink";
import {Branch, Tag} from "../generated/utils/api/models";

function TableHead(props: {
  branches: Array<Branch>,
  tags: Array<Tag>,
  currentRef: string,
  defaultBranch: string,
  path: Array<string>
}) {
  if (!props.currentRef) {
    return (<div />);
  }

  let additional;
  if (props.path.length === 0) {
    additional = (
      <Fragment key="icons">
        <Nav.Item><Nav.Link><DeviceHubIcon/>{props.branches.length}</Nav.Link></Nav.Item>
        <Nav.Item><Nav.Link><LocalOfferIcon/>{props.tags.length}</Nav.Link></Nav.Item>
      </Fragment>
  );
  } else {
    additional = (
      <Nav.Item className="nav-link">
        <ExploreLink toRef={props.currentRef}>nessie</ExploreLink>
        {
          props.path.map((p, index) => {
            return (
              <Fragment key={"path" + index}>
                <span style={{"paddingLeft":"0.5em", "paddingRight":"0.5em"}}>/</span>
                <ExploreLink key={index} toRef={props.currentRef} path={props.path.slice(0,index+1)}>{p}</ExploreLink>
              </Fragment>);
          })}
      </Nav.Item>
    );
  }

  return (
    <Nav variant={"pills"} activeKey={1}>
      <NavDropdown title={props.currentRef} id="nav-dropdown" style={{"paddingLeft":"1em"}}>

        <NavDropdown.Item disabled={true}>Branches</NavDropdown.Item>

        {props.branches.map(branch => {
          return (
            <ExploreLink toRef={branch.name} path={props.path} type="CONTAINER" key={branch.name}>
              <NavDropdown.Item as={"button"} key={branch.name}>
                {branch.name}
                <span>
                  {(branch.name === props.defaultBranch) ? <Badge pill className="float-right" variant={"secondary"}>default</Badge> : ""}
                </span>
              </NavDropdown.Item>
            </ExploreLink>
          )
        })}
        <NavDropdown.Divider/>
        <NavDropdown.Item disabled={true}>Tags</NavDropdown.Item>
        {props.tags.map(tag => {
          return (
            <ExploreLink toRef={tag.name} path={props.path} type="CONTAINER" key={tag.name}>
              <NavDropdown.Item as={"button"} key={tag.name}>{tag.name}</NavDropdown.Item>
            </ExploreLink>
            )
        })}
      </NavDropdown>
      {additional}
    </Nav>
  )
}

TableHead.defaultProps = {
  currentRef: "main"
}

export default TableHead;

