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

import { Card } from "react-bootstrap";
import moment from "moment";
import React, { Fragment } from "react";
import { CommitMeta } from "../utils";
import "./CommitDetails.css";
import { EmptyMessageView } from "./Components";

const CommitDetails = (props: {
  commitDetails: CommitMeta | undefined;
  currentRef: string;
}): React.ReactElement => {
  const { commitDetails, currentRef } = props;

  const renderDetailsView = () => {
    if (!commitDetails) return null;
    const {
      message,
      author,
      authorTime,
      signedOffBy,
      properties,
      hash,
      committer,
      commitTime,
    } = commitDetails;
    const additionalProperties = Object.keys(properties)
      .map((data) => [data, properties[data]])
      .map(([k, v]) => `${k}: ${v}`)
      .join(";");
    const propertiesArr = additionalProperties.split(";");

    const authorHoursDiff = moment().diff(moment(authorTime), "hours");
    const commitHoursDiff = moment().diff(moment(commitTime), "hours");

    const authorTimeAgo =
      authorHoursDiff > 24
        ? moment(authorTime).format("YYYY-MM-DD, hh:mm a")
        : `${moment(authorTime).fromNow()}`;
    const commitTimeAgo =
      commitHoursDiff > 24
        ? moment(commitTime).format("YYYY-MM-DD, hh:mm a")
        : `${moment(commitTime).fromNow()}`;

    return (
      <Card.Body className="commitDetails">
        <Card.Header>
          {message && <Card.Title>{message}</Card.Title>}
          {author && (
            <Card.Text className={"ml-3"}>
              <span className={"ml-2"}>Authored by: </span>
              <span className={"ml-2"}>{author}</span>
              <span className={"ml-2"}>at</span>
              <span className={"ml-2"}>{authorTimeAgo}</span>
            </Card.Text>
          )}
          {signedOffBy && (
            <Card.Text className={"ml-3"}>
              <span className={"ml-2"}>Signed off by: </span>
              <span className={"ml-2"}>{signedOffBy}</span>
            </Card.Text>
          )}
          {committer && (
            <Card.Text className={"ml-3"}>
              <span className={"ml-2"}>Committed by: </span>
              <span className={"ml-2"}>{committer}</span>
              <span className={"ml-2"}>at</span>
              <span className={"ml-2"}>{commitTimeAgo}</span>
            </Card.Text>
          )}
          {propertiesArr?.length > 0 && (
            <div className={"ml-3"}>
              <span className={"ml-2"}>Properties: </span>
              <ul>
                {propertiesArr.map((add, index) => {
                  return (
                    <li className={"ml-4"} key={index}>
                      {add}
                    </li>
                  );
                })}
              </ul>
            </div>
          )}
        </Card.Header>
        <Card.Footer className="commitDetails__branchFooter">
          <img
            alt="branchIcon"
            width="22"
            height="22"
            src="/branchIcon.svg"
            className="mr-2"
          />
          <Card.Title>{currentRef}</Card.Title>
        </Card.Footer>
        <Card.Footer className="commitDetails__authorFooter">
          <div>
            <span className="commitDetails__authorName mr-1">{author}</span>
            <span>committed on {moment(commitTime).fromNow()}</span>
          </div>
          <span>commit {hash}</span>
        </Card.Footer>
      </Card.Body>
    );
  };

  if (!commitDetails) {
    return <EmptyMessageView />;
  }

  return <Fragment>{renderDetailsView()}</Fragment>;
};

export default CommitDetails;
