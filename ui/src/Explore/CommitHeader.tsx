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
import prettyMilliseconds from "pretty-ms";
import React from "react";
import { CommitMeta } from "../utils";

const CommitHeader: React.FunctionComponent<CommitMeta> = ({
  committer,
  author,
  hash,
  message,
  properties,
  commitTime,
}: CommitMeta) => {
  if (!hash) {
    return <Card.Header />;
  }
  const props = Object.keys(properties)
    .map((data) => [data, properties[data]])
    .map(([k, v]) => `${k}=${v}`)
    .join("; ");
  return (
    <Card.Header>
      <span className={"float-left"}>
        <span className="font-weight-bold">{committer ?? author}</span>
        <span>{message + " " + props}</span>
      </span>
      <span className={"float-right"}>
        <span className="font-italic">{hash?.slice(0, 8)}</span>
        <span className={"pl-3"}>
          {prettyMilliseconds(
            new Date().getTime() - (commitTime ?? new Date(0)).getTime(),
            { compact: true }
          )}
        </span>
      </span>
    </Card.Header>
  );
};

export default CommitHeader;
