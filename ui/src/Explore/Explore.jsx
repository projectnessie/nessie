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
import React, {useEffect, useState} from 'react';

import {authenticationService} from '../services';
import {Card, Container} from "react-bootstrap";
import createApi from "../utils/api"
import {useHistory, useParams} from "react-router-dom";
import TableHead from "./TableHead";
import TableListing from "./TableListing";


// TODO: move this to server-side. Filter to keys relevant for this view.
function filterKeys(path, keys) {
  let containers = new Set();
  let filteredKeys = keys.map(key => key.name.elements)
    .filter(name => {
      if (name.length <= path.length) {
        return false;
      }

      // make sure all key values match the current path.
      return name.slice(0, path.length).every((v, i) => v.toLowerCase() === path[i].toLowerCase());
    })
    .map(name => {
      let ele = name[path.length];
      if (name.length > path.length + 1) {
        containers.add(ele);
      }
      return ele;
    });

  let distinctKeys = [...new Set(filteredKeys)];
  let distinctObjs = distinctKeys.map(key => {
    return {
      name: key,
      type: containers.has(key) ? "CONTAINER" : "TABLE"
    };
  })
  return distinctObjs;
}

function fetchLog(currentUser, currentBranch) {
  return createApi({'cors': true}).getCommitLog({'ref': currentBranch})
    .then(res => {
      return res.json();
    })
    .then((data) => {
      return data.operations[0];
    });
}


function api() {
  return createApi({'cors': true});
}


function fetchKeys(ref, path, result) {
  return api().getEntries({'ref':ref})
    .then(res => {
      return res.json();
    })
    .then((data) => {
      //setKeys(data.)
      let keys = filterKeys(path, data.entries);
      return keys;
    });
}

function fetchDefaultBranch(currentUser) {
  return api().getDefaultBranch()
    .then(res => {
      return res.json();
    })
    .then((data) => {
      return data.name;
    })
}

function loadBranchesAndTags() {
  return api().getAllReferences()
    .then(res => {
      return res.json();
    })
    .then((data) => {
      return {
        branches: data.filter(x => x.type === "BRANCH"),
        tags: data.filter(x => x.type === "TAG")
      }
    });
}

function parseSlug(slug, defaultBranch, branches, tags) {

  if (!slug || slug.split("/").length === 0) {
    return {currentRef: defaultBranch, path: []};
  }

  // if we have a slug, need to figure out what portion is related to a ref versus a key.
  let sub = "";
  let checker = b => {

    if (b.name.length <= sub.length) {
      return;
    }
    if (slug.toLowerCase().startsWith(b.name.toLowerCase())) {
      sub = b.name;
    }
  };

  branches.forEach(checker);
  tags.forEach(checker);

  if (sub.length === 0) {
    sub = slug.split("/")[0];
  }

  let path = slug.slice(sub.length).split("/").filter(i => i);

  return {currentRef: sub, path: path};
}

async function updateState(currentUser, slug, setDefaultBranch, setBranches, setTags, setPath, setKeys, setCurrentRef, setLog) {
  const defaultBranch = await fetchDefaultBranch(currentUser);
  const branchesAndTags = await loadBranchesAndTags();
  //const [defaultBranch, branchesAndTags] = Promise.all([t => fetchDefaultBranch(currentUser), loadBranchesAndTags]);
  setDefaultBranch(defaultBranch);
  setBranches(branchesAndTags.branches);
  setTags(branchesAndTags.tags);
  const {currentRef, path} = parseSlug(slug, defaultBranch, branchesAndTags.branches, branchesAndTags.tags);
  const log = await fetchLog(currentUser, currentRef);
  setLog(log);
  setCurrentRef(currentRef);
  setPath(path);
  const keys = await fetchKeys(currentRef, path);
  setKeys(keys);
}

function Explore(props) {

  let { slug } = useParams();
  let history = useHistory();
  const currentUser = authenticationService.currentUserValue;
  //const [currentRef, setCurrentRef] = useState("main");

  const [defaultBranch, setDefaultBranch] = useState("main");
  const [branches, setBranches] = useState([]);
  const [tags, setTags] = useState([]);
  const [path, setPath] = useState([]);
  const [keys, setKeys] = useState([]);
  const [log, setLog] = useState();
  const [currentRef, setCurrentRef] = useState("main");

  useEffect(() => {
    updateState(currentUser, slug, setDefaultBranch, setBranches, setTags, setPath, setKeys, setCurrentRef, setLog);
  }, [slug]);
  // eslint-disable-next-line react-hooks/exhaustive-deps


  return (
    <div>
      <Container style={{"marginTop": "100px"}}>
        <Card>
          <TableHead
            branches={branches}
            tags={tags}
            currentRef={currentRef}
            defaultBranch={defaultBranch}
            path={path}
             />
          <TableListing keys={keys} currentRef={currentRef} path={path} history={history} currentLog={log} />
        </Card>
      </Container>
    </div>
  );
}

Explore.propTypes = {}

export default Explore;
