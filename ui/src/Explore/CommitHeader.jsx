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

import {Card} from "react-bootstrap";
import prettyMilliseconds from "pretty-ms";
import React, {useEffect, useState} from "react";
import PropTypes from "prop-types";
import createApi from "../utils/api";

function fetchLog(currentRef, setLog) {
  return createApi({'cors': true}).getCommitLog({'ref': currentRef})
    .then(res => {
      return res.json();
    })
    .then((data) => {
      if(data.operations && data.operations.length > 0) {
        setLog(data.operations[0]);
      }
    }).catch(t => console.log(t));
}

function CommitHeader(props) {

  const [currentLog, setLog] = useState();

  useEffect(() => {
    fetchLog(props.currentRef, setLog);
  }, [props.currentRef, props.path])

  if (!currentLog) {
    return (<Card.Header/>)
  }
  return (<Card.Header>
    <span className={"float-left"}>
      <span className="font-weight-bold">{currentLog.committer}</span>
      <span>{currentLog.message}</span>
    </span>
    <span className={"float-right"}>
      <span className="font-italic">{currentLog.hash.slice(0,8)}</span>
      <span
        className={"pl-3"}>{prettyMilliseconds(new Date().getTime() - currentLog.commitTime, {compact: true})}</span>
    </span>
  </Card.Header>)
}

CommitHeader.propTypes = {
  currentRef: PropTypes.string.isRequired,
}

CommitHeader.defaultProps = {
  currentRef: "main"
}

export default CommitHeader;
