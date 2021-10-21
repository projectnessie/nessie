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
import React, { useEffect, useState, Fragment } from "react";
import { useParams, useHistory, useLocation } from "react-router-dom";
import { api, CommitMeta } from "../utils";
import { factory } from "../ConfigLog4j";
import CodeIcon from "@material-ui/icons/Code";
import "./CommitLog.css";
import { Icon, Tooltip } from "@material-ui/core";
import ExploreLink from "./ExploreLink";
import { routeSlugs } from "./Constants";
import { EmptyMessageView } from "./Components";
import CommitDetails from "./CommitDetails";

const log = factory.getLogger("api.CommitHeader");

const fetchLog = (ref: string): Promise<void | CommitMeta[] | undefined> => {
  return api()
    .getCommitLog({ ref })
    .then((data) => {
      if (data.operations && data.operations.length > 0) {
        return data.operations;
      }
    })
    .catch((t) => log.error("CommitLog", t));
};

const CommitLog = (props: {
  currentRef: string;
  path: string[];
}): React.ReactElement => {
  const { currentRef, path } = props;
  const { slug } = useParams<{ slug: string }>();
  const [logList, setLogList] = useState<CommitMeta[]>([
    {
      author: undefined,
      authorTime: undefined,
      commitTime: undefined,
      committer: undefined,
      hash: undefined,
      message: "",
      properties: {},
      signedOffBy: undefined,
    },
  ]);
  const [showCommitDetails, setShowCommitDetails] = useState(false);
  const [commitDetails, setCommitDetails] = useState<CommitMeta | undefined>();
  const location = useLocation();
  const history = useHistory();
  useEffect(() => {
    const logs = async () => {
      const results = await fetchLog(currentRef);
      if (results) {
        setLogList(results);
      }
      if (showCommitDetails) {
        const listPath = location.pathname.substring(
          0,
          location.pathname.lastIndexOf("/")
        );
        history.push(listPath);
      }
    };
    void logs();
  }, [currentRef]);

  useEffect(() => {
    if (slug) {
      const last = slug.substring(slug.lastIndexOf("/") + 1, slug.length);
      setShowCommitDetails(last !== routeSlugs.commits);
      const logDetails = logList.find((logItem) => logItem.hash === last);
      setCommitDetails(logDetails);
    }
  }, [slug, logList]);

  if (!logList || (logList.length === 1 && !logList[0].hash)) {
    return <EmptyMessageView />;
  }

  const copyHash = async (hashCode: string) => {
    await navigator.clipboard.writeText(hashCode);
  };
  const commitList = (currentLog: CommitMeta, index: number) => {
    const { commitTime, author, message, hash } = currentLog;
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
                toRef={currentRef}
                path={path.concat(hash ?? "#")}
                type="CONTAINER"
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
                toRef={currentRef}
                path={path.concat(hash ?? "#")}
                type="CONTAINER"
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
                <Button variant="light" className={"ml-3 rightBtnHover"}>
                  <CodeIcon />
                </Button>
              </Tooltip>
            </div>
          </div>
        </Card.Body>
      </Fragment>
    );
  };

  return (
    <Card className={"commitLog"}>
      {!showCommitDetails ? (
        logList.map((item, index) => {
          return commitList(item, index);
        })
      ) : (
        <CommitDetails commitDetails={commitDetails} currentRef={currentRef} />
      )}
    </Card>
  );
};

export default CommitLog;
