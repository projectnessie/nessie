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
import {useParams} from "react-router-dom";
import TableHead from "./TableHead";
import TableListing from "./TableListing";
import CommitLog from "./CommitHeader";
import {api, Branch, Tag} from "../utils";


function fetchDefaultBranch(setDefaultBranch: (prev: string) => void) {
  return api().getDefaultBranch()
    .then((data) => {
      data.name && setDefaultBranch(data.name);
    }).catch(t => console.log(t))
}

function btCompare(a: Branch, b: Branch) {
  if (!a.name) {
    if (!b.name) {
      return 0;
    } else {
      return 1;
    }
  } else if(!b.name) {
    return -1;
  }

  if (a.name < b.name) {
    return -1;
  }
  if (a.name > b.name) {
    return 1;
  }

  return 0;
}

function loadBranchesAndTags(
  setBranches: (val: Array<Branch>) => void,
  setTags: (val: Array<Tag>) => void) {
  return api().getAllReferences()
    .then((data) => {
      data = data.sort(btCompare);
      setBranches(data.filter(x => x.type === "BRANCH").map(b => b as Branch));
      setTags(data.filter(x => x.type === "TAG").map(t => t as Tag));
    });
}

function parseSlug(slug: string, defaultBranch: string, branches: Array<Branch>, tags: Array<Tag>) {

  if (!slug || slug.split("/").length === 0) {
    return {currentRef: defaultBranch, path: []};
  }

  // if we have a slug, need to figure out what portion is related to a ref versus a key.
  let sub: string = "";
  let checker = (b: Branch) => {
    if(!b.name) {
      return;
    }
    if (b.name.length <= sub.length) {
      return;
    }
    if (slug.toLowerCase().startsWith(b.name.toLowerCase())) {
      sub = b.name  ?? "";
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

function updateRef(
  slug: string,
  defaultBranch: string,
  branches: Array<Branch>,
  tags: Array<Tag>,
  setPath: (v: Array<string>) => void,
  setCurrentRef: (v: string) => void
) {
  if (!defaultBranch || !branches) {
    return;
  }

  const {currentRef, path} = parseSlug(slug, defaultBranch, branches, tags);
  setCurrentRef(currentRef);
  setPath(path);
}

function Explore(props: {}) {

  let { slug } = useParams<{slug: string}>();
  const [defaultBranch, setDefaultBranch] = useState<string>("main");
  const [branches, setBranches] = useState<Array<Branch>>([]);
  const [tags, setTags] = useState<Array<Tag>>([]);
  const [path, setPath] = useState<Array<string>>([]);
  const [currentRef, setCurrentRef] = useState<string>();

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
