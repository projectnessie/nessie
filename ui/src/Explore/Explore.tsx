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
import React, { useEffect, useState } from "react";
import { Card, Container } from "react-bootstrap";
import { useParams } from "react-router-dom";
import TableHead from "./TableHead";
import TableListing from "./TableListing";
import CommitHeader from "./CommitHeader";
import { api, Branch, CommitMeta, Tag } from "../utils";
import { factory } from "../ConfigLog4j";

const log = factory.getLogger("api.Explore");

const fetchDefaultBranch = (
  setDefaultBranch: (prev: string) => void
): Promise<void> => {
  return api()
    .getDefaultBranch()
    .then((data) => {
      if (data.name) {
        setDefaultBranch(data.name);
      }
    })
    .catch((t) => log.error("DefaultBranch", t));
};

const btCompare = (a: Branch, b: Branch): number => {
  if (!a.name) {
    return !b.name ? 0 : 1;
  } else if (!b.name) {
    return -1;
  }

  if (a.name < b.name) {
    return -1;
  }
  if (a.name > b.name) {
    return 1;
  }

  return 0;
};

const loadBranchesAndTags = (
  setBranches: (val: Branch[]) => void,
  setTags: (val: Tag[]) => void
): Promise<void> => {
  return api()
    .getAllReferences()
    .then((data) => {
      data = data.sort(btCompare);
      setBranches(
        data.filter((x) => x.type === "BRANCH").map((b) => b as Branch)
      );
      setTags(data.filter((x) => x.type === "TAG").map((t) => t as Tag));
    });
};

interface Slug {
  currentRef: string;
  path: string[];
}

const parseSlug = (
  slug: string,
  defaultBranch: string,
  branches: Branch[],
  tags: Tag[]
): Slug => {
  if (!slug || slug.split("/").length === 0) {
    return { currentRef: defaultBranch, path: [] };
  }

  // if we have a slug, need to figure out what portion is related to a ref versus a key.
  let sub = "";
  const checker = (b: Branch) => {
    if (!b.name) {
      return;
    }
    if (b.name.length <= sub.length) {
      return;
    }
    if (slug.toLowerCase().startsWith(b.name.toLowerCase())) {
      sub = b.name ?? "";
    }
  };

  branches.forEach(checker);
  tags.forEach(checker);

  if (sub.length === 0) {
    sub = slug.split("/")[0];
  }

  const path = slug
    .slice(sub.length)
    .split("/")
    .filter((i) => i);

  return { currentRef: sub, path };
};

const updateRef = (
  slug: string,
  defaultBranch: string,
  branches: Branch[],
  tags: Tag[],
  setPath: (v: string[]) => void,
  setCurrentRef: (v: string) => void
): void => {
  if (!defaultBranch || !branches) {
    return;
  }

  const { currentRef, path } = parseSlug(slug, defaultBranch, branches, tags);
  setCurrentRef(currentRef);
  setPath(path);
};

const fetchLog = (
  currentRef: string,
  setLog: (v: CommitMeta) => void
): Promise<void> => {
  return api()
    .getCommitLog({ ref: currentRef })
    .then((data) => {
      if (data.operations && data.operations.length > 0) {
        setLog(data.operations[0]);
      }
    })
    .catch((t) => log.error("CommitLog", t));
};

const Explore = (): React.ReactElement => {
  const { slug } = useParams<{ slug: string }>();
  const [defaultBranch, setDefaultBranch] = useState<string>("main");
  const [branches, setBranches] = useState<Branch[]>([]);
  const [tags, setTags] = useState<Tag[]>([]);
  const [path, setPath] = useState<string[]>([]);
  const [currentRef, setCurrentRef] = useState<string>();
  const [currentLog, setLog] = useState<CommitMeta>({
    author: undefined,
    authorTime: undefined,
    commitTime: undefined,
    committer: undefined,
    hash: undefined,
    message: "",
    properties: {},
    signedOffBy: undefined,
  });

  useEffect(() => {
    if (currentRef) {
      void fetchLog(currentRef, setLog);
    }
  }, [currentRef]);

  useEffect(() => {
    void fetchDefaultBranch(setDefaultBranch);
    void loadBranchesAndTags(setBranches, setTags);
  }, []);

  useEffect(() => {
    updateRef(slug, defaultBranch, branches, tags, setPath, setCurrentRef);
  }, [slug, defaultBranch, branches, tags]);

  return (
    <div>
      <Container style={{ marginTop: "100px" }}>
        <Card>
          <TableHead
            branches={branches}
            tags={tags}
            currentRef={currentRef}
            defaultBranch={defaultBranch}
            path={path}
          />
          <CommitHeader
            committer={currentLog.committer}
            properties={currentLog.properties}
            message={currentLog.message}
            commitTime={currentLog.commitTime}
            author={currentLog.author}
            hash={currentLog.hash}
          />
          <TableListing currentRef={currentRef} path={path} />
        </Card>
      </Container>
    </div>
  );
};

Explore.propTypes = {};

export default Explore;
