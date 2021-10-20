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
import React, { Fragment } from "react";
import { Badge, Nav, NavDropdown } from "react-bootstrap";
import DeviceHubIcon from "@material-ui/icons/DeviceHub";
import LocalOfferIcon from "@material-ui/icons/LocalOffer";
import HistoryIcon from "@material-ui/icons/History";
import ExploreLink from "./ExploreLink";
import { Branch, Tag } from "../generated/utils/api";
import { routeSlugs } from "./Constants";
import "./TableHead.css";

interface ITableHeadProps {
  branches: Branch[];
  tags: Tag[];
  currentRef?: string;
  defaultBranch: string;
  path?: string[];
}

const TableHead = ({
  branches,
  tags,
  currentRef,
  defaultBranch,
  path,
}: ITableHeadProps): React.ReactElement => {
  if (!currentRef) {
    return <div />;
  }
  path = path || [];

  const rightContent = path.length === 0 && (
    <Nav.Item className="tableHead__rightContentWrapper">
      <ExploreLink
        toRef={currentRef}
        path={path.concat(routeSlugs.commits)}
        type="CONTAINER"
        className="nav-link"
      >
        <>
          <HistoryIcon />
          Commit History
        </>
      </ExploreLink>
    </Nav.Item>
  );

  const additional =
    path.length === 0 ? (
      <Fragment key="icons">
        <Nav.Item>
          <Nav.Link>
            <DeviceHubIcon />
            {branches.length}
          </Nav.Link>
        </Nav.Item>
        <Nav.Item>
          <Nav.Link>
            <LocalOfferIcon />
            {tags.length}
          </Nav.Link>
        </Nav.Item>
      </Fragment>
    ) : (
      <Nav.Item className="nav-link">
        <ExploreLink toRef={currentRef}>nessie</ExploreLink>
        {path.map((p, index) => {
          return (
            <Fragment key={`path${index}`}>
              <span style={{ paddingLeft: "0.5em", paddingRight: "0.5em" }}>
                /
              </span>
              <ExploreLink
                key={index}
                toRef={currentRef}
                path={(path || []).slice(0, index + 1)}
              >
                {p}
              </ExploreLink>
            </Fragment>
          );
        })}
      </Nav.Item>
    );

  return (
    <Nav variant={"pills"} activeKey={1} className="tableHead">
      <div className="tableHead__leftContentWrapper">
        <NavDropdown
          title={currentRef}
          id="nav-dropdown"
          style={{ paddingLeft: "1em" }}
        >
          <NavDropdown.Item disabled>Branches</NavDropdown.Item>

          {branches.map((branch) => {
            return (
              <ExploreLink
                toRef={branch.name}
                path={path}
                type="CONTAINER"
                key={branch.name}
              >
                <NavDropdown.Item as={"button"} key={branch.name}>
                  {branch.name}
                  <span>
                    {branch.name === defaultBranch ? (
                      <Badge pill className="float-right" variant={"secondary"}>
                        default
                      </Badge>
                    ) : (
                      ""
                    )}
                  </span>
                </NavDropdown.Item>
              </ExploreLink>
            );
          })}
          <NavDropdown.Divider />
          <NavDropdown.Item disabled>Tags</NavDropdown.Item>
          {tags.map((tag) => {
            return (
              <ExploreLink
                toRef={tag.name}
                path={path}
                type="CONTAINER"
                key={tag.name}
              >
                <NavDropdown.Item as={"button"} key={tag.name}>
                  {tag.name}
                </NavDropdown.Item>
              </ExploreLink>
            );
          })}
        </NavDropdown>
        {additional}
      </div>
      {rightContent}
    </Nav>
  );
};

export default TableHead;
