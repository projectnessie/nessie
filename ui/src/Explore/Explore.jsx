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
import {Card, Container} from "react-bootstrap";
import createApi from "../utils/api"
import {useParams} from "react-router-dom";
import TableHead from "./TableHead";
import TableListing from "./TableListing";
import CommitLog from "./CommitHeader";



function api() {
  return createApi({'cors': true});
}



function fetchDefaultBranch(setDefaultBranch) {
  //const currentUser = authenticationService.currentUserValue;
  return api().getDefaultBranch()
    .then(res => {
      return res.json();
    })
    .then((data) => {
      setDefaultBranch(data.name);
    }).catch(t => console.log(t))
}

function loadBranchesAndTags(setBranches, setTags) {
  return api().getAllReferences()
    .then(res => {
      return res.json();
    })
    .then((data) => {
      setBranches(data.filter(x => x.type === "BRANCH"));
      setTags(data.filter(x => x.type === "TAG"));
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

function updateRef(slug, defaultBranch, branches, tags, setPath, setCurrentRef) {
  if (!defaultBranch || !branches) {
    return;
  }

  const {currentRef, path} = parseSlug(slug, defaultBranch, branches, tags);
  setCurrentRef(currentRef);
  setPath(path);
}


function Explore(props) {

  let { slug } = useParams();
  //const [currentRef, setCurrentRef] = useState("main");

  const [defaultBranch, setDefaultBranch] = useState("main");
  const [branches, setBranches] = useState([]);
  const [tags, setTags] = useState([]);
  const [path, setPath] = useState([]);
  const [currentRef, setCurrentRef] = useState("main");

  useEffect(() => {
    fetchDefaultBranch(setDefaultBranch);
    loadBranchesAndTags(setBranches, setTags);
  }, []);

  useEffect(() => {
    updateRef(slug, defaultBranch, branches, tags, setPath, setCurrentRef);
  }, [slug, defaultBranch, branches, tags]);

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
          <CommitLog currentRef={currentRef} />
          <TableListing currentRef={currentRef} path={path} />
        </Card>
      </Container>
    </div>
  );
}

Explore.propTypes = {}

export default Explore;
