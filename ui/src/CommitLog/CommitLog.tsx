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

import { Button, Card, Nav } from "react-bootstrap";
import moment from "moment";
import React, { Fragment } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { LogEntry } from "../utils";
import CodeIcon from "@material-ui/icons/Code";
import "./CommitLog.css";
import { Icon, TablePagination, Tooltip } from "@material-ui/core";
import { ExploreLink } from "../ExploreLink";
import { EmptyMessageView } from "../EmptyMessageView";

type CommitLogProps = {
  handleChangePage: (
    event: React.MouseEvent<HTMLButtonElement> | null,
    newPage: number
  ) => void;
  handleChangeRowsPerPage: (
    event: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => void;
  logList: LogEntry[];
  page: number;
  rowsPerPage: number;
  hasMoreLog: boolean;
};

const CommitLog = ({
  handleChangePage,
  handleChangeRowsPerPage,
  page,
  rowsPerPage,
  hasMoreLog,
  logList,
}: CommitLogProps): React.ReactElement => {
  const history = useNavigate();
  const { branch } = useParams<{ branch: string }>();

  if (!branch) {
    return <EmptyMessageView />;
  }

  if (!logList || logList.length === 0 || !logList[0].commitMeta.hash) {
    return <EmptyMessageView />;
  }

  const copyHash = (hashCode: string) => {
    (async () => {
      await navigator.clipboard.writeText(hashCode);
    })().catch((failed) => alert(failed));
  };
  const commitList = (currentLog: LogEntry, index: number) => {
    const { commitTime, author, message, hash } = currentLog.commitMeta;
    const hoursDiff = moment().diff(moment(commitTime), "hours");
    const dateTimeAgo =
      hoursDiff > 24
        ? moment(commitTime).format("YYYY-MM-DD, hh:mm a")
        : `${moment(commitTime).fromNow()}`;

    return (
      <Fragment key={index}>
        <Card.Body className="commitLog__body border-bottom">
          <div>
            <Nav.Item>
              <ExploreLink
                toRef={branch}
                path={(hash ?? "#").split("/")}
                type="COMMIT"
                className="commitLog__messageLink"
              >
                {message}
              </ExploreLink>
            </Nav.Item>
            <Card.Text className={"ml-3"}>
              {author}
              <span className={"ml-2"}>committed on</span>
              <span className={"ml-2"}>{dateTimeAgo}</span>
            </Card.Text>
          </div>
          <div className={"commitLog__btnWrapper"}>
            <div className={"border rounded commitLog__hashBtnWrapper"}>
              <Tooltip title="Copy hash">
                <Button
                  className="border-right commitLog__copyBtn rightBtnHover"
                  variant="link"
                  onClick={() => copyHash(hash || "")}
                >
                  <Icon>content_copy</Icon>
                </Button>
              </Tooltip>
              <ExploreLink
                toRef={branch}
                path={(hash ?? "#").split("/")}
                type="COMMIT"
                className="commitLog__hashBtn rightBtnHover"
              >
                <Tooltip title="Commit details">
                  <Button variant="link">
                    <span className="font-italic">{hash?.slice(0, 8)}</span>
                  </Button>
                </Tooltip>
              </ExploreLink>
            </div>
            <div>
              <Tooltip title="Browse the repository at this point in the history">
                <Button
                  variant="light"
                  className={"ml-3 rightBtnHover"}
                  onClick={() => history(`/tree/${branch}:${hash as string}`)}
                >
                  <CodeIcon />
                </Button>
              </Tooltip>
            </div>
          </div>
        </Card.Body>
      </Fragment>
    );
  };

  const paginator = () => (
    <TablePagination
      component="div"
      count={!hasMoreLog ? logList.length : -1}
      page={page}
      onPageChange={handleChangePage}
      rowsPerPage={rowsPerPage}
      onRowsPerPageChange={handleChangeRowsPerPage}
      SelectProps={{
        id: "commitLogRowPerPageSelect",
        labelId: "commitLogRowPerPageSelectLabel",
      }}
    />
  );

  return (
    <React.Fragment>
      {logList
        .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
        .map((item, index) => {
          return commitList(item, index);
        })}
      {paginator()}
    </React.Fragment>
  );
};

export default CommitLog;
