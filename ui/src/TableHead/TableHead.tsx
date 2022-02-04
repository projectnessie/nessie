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
import { ExploreLink } from "../ExploreLink";
import { Branch, Tag } from "../generated/utils/api";
import "./TableHead.css";

interface ITableHeadProps {
  branches: Branch[];
  tags: Tag[];
  currentRef?: string;
  defaultBranch: string;
  path?: string[];
}

type NavDropdownProps = {
  branches: Branch[];
  tags: Tag[];
  currentRef: string;
  defaultBranch: string;
  path: string[];
  type: "CONTAINER" | "OBJECT" | "COMMIT";
};

const BranchesDropdown = ({
  branches,
  tags,
  currentRef,
  defaultBranch,
  path,
  type,
}: NavDropdownProps): React.ReactElement => {
  return (
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
            type={type}
            key={branch.name}
          >
            <NavDropdown.Item as={"button"} key={branch.name}>
              {branch.name}
              <span>
                {branch.name === defaultBranch ? (
                  <Badge pill className="float-right" bg={"secondary"}>
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
  );
};

const TreeTableHead = ({
  branches,
  tags,
  currentRef,
  defaultBranch,
  path,
}: ITableHeadProps): React.ReactElement => {
  if (!currentRef) {
    return <div />;
  }
  return (
    <Nav variant={"pills"} activeKey={1} className="tableHead">
      <div className="tableHead__leftContentWrapper">
        <BranchesDropdown
          branches={branches}
          tags={tags}
          currentRef={currentRef}
          defaultBranch={defaultBranch}
          path={path || []}
          type={"CONTAINER"}
        />
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
      </div>
      <Nav.Item className="tableHead__rightContentWrapper">
        <ExploreLink
          toRef={currentRef}
          path={path?.concat("commits") || ["commits"]}
          type="COMMIT"
          className="nav-link"
        >
          <>
            <HistoryIcon />
            Commit History
          </>
        </ExploreLink>
      </Nav.Item>
    </Nav>
  );
};

type PathTableHeadProps = {
  currentRef: string;
  path: string[];
};

const PathTableHead = ({
  currentRef,
  path,
}: PathTableHeadProps): React.ReactElement => {
  return (
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
};

const PathCommitTableHead = ({
  currentRef,
  path,
}: PathTableHeadProps): React.ReactElement => {
  return (
    <Nav.Item className="nav-link">
      <ExploreLink toRef={currentRef}>nessie</ExploreLink>
      <span style={{ paddingLeft: "0.5em", paddingRight: "0.5em" }}>/</span>
      <ExploreLink toRef={currentRef} type={"COMMIT"}>
        commits
      </ExploreLink>
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
              type={"COMMIT"}
            >
              {p}
            </ExploreLink>
          </Fragment>
        );
      })}
    </Nav.Item>
  );
};

const PathTreeTableHead = ({
  branches,
  tags,
  currentRef,
  defaultBranch,
  path,
}: ITableHeadProps): React.ReactElement => {
  if (!currentRef) {
    return <div />;
  }

  return (
    <Nav variant={"pills"} activeKey={1} className="tableHead">
      <div className="tableHead__leftContentWrapper">
        <BranchesDropdown
          branches={branches}
          tags={tags}
          currentRef={currentRef}
          defaultBranch={defaultBranch}
          path={path as string[]}
          type={"COMMIT"}
        />
        <PathTableHead currentRef={currentRef} path={path || []} />
      </div>
    </Nav>
  );
};

const CommitTableHead = ({
  branches,
  tags,
  currentRef,
  defaultBranch,
  path,
}: ITableHeadProps): React.ReactElement => {
  if (!currentRef) {
    return <div />;
  }

  return (
    <Nav variant={"pills"} activeKey={1} className="tableHead">
      <div className="tableHead__leftContentWrapper">
        <BranchesDropdown
          branches={branches}
          tags={tags}
          currentRef={currentRef}
          defaultBranch={defaultBranch}
          path={path as string[]}
          type={"COMMIT"}
        />
        <PathCommitTableHead currentRef={currentRef} path={path || []} />
      </div>
    </Nav>
  );
};

export { CommitTableHead, TreeTableHead, PathTreeTableHead };
