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
import React, { useEffect, useState } from "react";
import { api, LogEntry } from "../utils";
import { logProvider } from "../ConfigLog4j";

const log = logProvider.getLogger("api.CommitHeader");

const fetchLog = (currentRef: string): Promise<void | LogEntry | undefined> => {
  return api()
    .getCommitLog({ ref: currentRef })
    .then((data) => {
      if (data.logEntries && data.logEntries.length > 0) {
        return data.logEntries[0];
      }
    })
    .catch((t) => log.error("CommitLog", t as undefined));
};

const CommitHeader = (props: { currentRef: string }): React.ReactElement => {
  const [currentLog, setLog] = useState<LogEntry>({
    commitMeta: {
      author: undefined,
      authorTime: undefined,
      commitTime: undefined,
      committer: undefined,
      hash: undefined,
      message: "",
      properties: {},
      signedOffBy: undefined,
    },
  });
  useEffect(() => {
    const logs = async () => {
      const results = await fetchLog(props.currentRef);
      if (results) {
        setLog(results);
      }
    };

    void logs();
  }, [props.currentRef]);

  if (!currentLog || !currentLog.commitMeta.hash) {
    return <Card.Header />;
  }
  const properties = Object.keys(currentLog.commitMeta.properties)
    .map((data) => [data, currentLog.commitMeta.properties[data]])
    .map(([k, v]) => `${k}=${v}`)
    .join("; ");
  return (
    <Card.Header>
      <span className={"float-left"}>
        <span className="font-weight-bold">
          {currentLog.commitMeta.committer ?? currentLog.commitMeta.author}
        </span>
        <span>{currentLog.commitMeta.message + " " + properties}</span>
      </span>
      <span className={"float-right"}>
        <span className="font-italic">
          {currentLog.commitMeta.hash?.slice(0, 8)}
        </span>
        <span className={"pl-3"}>
          {prettyMilliseconds(
            new Date().getTime() -
              (currentLog.commitMeta.commitTime ?? new Date(0)).getTime(),
            { compact: true }
          )}
        </span>
      </span>
    </Card.Header>
  );
};

export default CommitHeader;
