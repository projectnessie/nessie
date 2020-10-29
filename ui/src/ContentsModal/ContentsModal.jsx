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
import React, {useState, useEffect} from 'react';

import {Row,Col} from "react-bootstrap";
import createApi from "../utils/api"
import Modal from "react-bootstrap/Modal";

function getContents(x, currentBranch, setContent) {
  if (x === undefined || (x.length === 1 && x[0] === undefined)) {
    return;
  }

  const key = x.map(x => x.replace('.', '\u0000')).join(".");
  createApi({'cors': true}).getContents({'ref': currentBranch.name, 'key': key})
    .then(res => {
      return res.json();
    })
    .then((data) => {
      setContent(data);
    })
    .catch(console.log);
}

function image(currentContent) {
  if (currentContent.type === "ICEBERG_TABLE") {
    return (<Row><Col xs={3}>Table type:</Col> <Col>Iceberg Table<img className="pl-2" src={"/iceberg.png"}
                                                                      alt={"iceberg table"} height={"50"} width={"60"}/></Col></Row>)
  } else if (currentContent.type === "DELTA_LAKE_TABLE") {
    return (<Row><Col xs={3}>Table type:</Col> <Col>Delta Lake Table<img className="pl-4" src={"/delta.png"}
                                                                         alt={"iceberg table"} height={"50"}
                                                                         width={"50"}/></Col></Row>)
  } else if (currentContent.type === "HIVE_TABLE" || currentContent.type === "HIVE_DATABASE") {
    return (<Row><Col xs={3}>Table type:</Col>
      <Col>Hive {(currentContent.type === "HIVE_DATABASE") ? "Database" : "Table"}<img className="pl-4"
                                                                                       src={"/hive.png"}
                                                                                       alt={"iceberg table"}
                                                                                       height={"50"}
                                                                                       width={"50"}/></Col></Row>)
  } else {
    return (<span>{currentContent.type}</span>)
  }
}

function detail(currentContent) {
  if (currentContent.type === "ICEBERG_TABLE") {
    return (<Row><Col xs={2}>Metadata Location:</Col><Col>{currentContent.metadataLocation}</Col></Row>)
  } else if (currentContent.type === "DELTA_LAKE_TABLE") {
    return (<Row><Col xs={2}>Metadata Location:</Col><Col>{currentContent.metadataLocation}</Col></Row>)
  } else if (currentContent.type === "HIVE_TABLE" || currentContent.type === "HIVE_DATABASE") {
    return (<Row><Col xs={2}>Metadata Location:</Col><Col>{currentContent.metadataLocation}</Col></Row>)
  } else {
    return (<span>{JSON.stringify(currentContent)}</span>)
  }
}


function ContentsModal(props) {
  let name = (props.currentKey.parts === undefined) ? [] : props.currentKey.parts.slice();
  name.push(props.currentKey.name);
  const [currentContent, setContent] = useState([]);
  useEffect(() => {
    getContents(name, props.currentBranch, setContent);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [props.currentKey])

  return (
    <Modal show={props.show} onHide={props.handleClose} dialogClassName="modal-90w" size="large">
      <Modal.Header closeButton>
        <Modal.Title>{ `${name.join(".")} on ${props.currentBranch.name} `}</Modal.Title>
      </Modal.Header>
      <Modal.Body>
        {image(currentContent)}
        {detail(currentContent)}
      </Modal.Body>
    </Modal>
  )
}

export {ContentsModal};
