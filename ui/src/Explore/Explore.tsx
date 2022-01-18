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
import { routeSlugs } from "./Constants";
import CommitLog from "./CommitLog";
import { api, Branch, Reference, Tag } from "../utils";
import { factory } from "../ConfigLog4j";
import "./Explore.css";

const log = factory.getLogger("api.Explore");

const fetchDefaultBranch = (): Promise<string | void> => {
  return api()
    .getDefaultBranch()
    .then((data) => {
      if (data.name) {
        return data.name;
      }
    })
    .catch((t) => log.error("DefaultBranch", t as undefined));
};

const loadBranchesAndTags = (): Promise<Reference[] | void> => {
  return api()
    .getAllReferences({})
    .then((data) => {
      return data.references.sort(btCompare);
    })
    .catch((t) => log.error("AllReferences", t as undefined));
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
  tags: Tag[]
): Slug | void => {
  if (!defaultBranch || !branches) {
    return;
  }

  return parseSlug(slug, defaultBranch, branches, tags);
};

interface IBranchState {
  branches: Branch[];
  tags: Tag[];
}

const Explore = (): React.ReactElement => {
  const { slug } = useParams<{ slug: string }>();
  const [defaultBranch, setDefaultBranch] = useState<string>("main");
  const [branches, setBranches] = useState<IBranchState>({
    branches: [],
    tags: [],
  });
  const [path, setPath] = useState<string[]>([]);
  const [currentRef, setCurrentRef] = useState<string>();

  useEffect(() => {
    const references = async () => {
      const results = await loadBranchesAndTags();
      if (results) {
        setBranches({
          branches: results
            .filter((x) => x.type === "BRANCH")
            .map((b) => b as Branch),
          tags: results.filter((x) => x.type === "TAG").map((t) => t as Tag),
        });
      }
    };
    const getDefaultBranch = async () => {
      const results = await fetchDefaultBranch();
      if (results) {
        setDefaultBranch(results);
      }
    };
    void references();
    void getDefaultBranch();
  }, []);

  useEffect(() => {
    if (branches.branches.length === 0 && branches.tags.length === 0) {
      return;
    }
    const newSlug = updateRef(
      slug,
      defaultBranch,
      branches.branches,
      branches.tags
    );
    if (newSlug) {
      setCurrentRef(newSlug.currentRef);
      setPath(newSlug.path);
    }
  }, [slug, defaultBranch, branches]);

  return currentRef ? (
    <div>
      <Container className="explore">
        <Card className="explore__wrapper">
          <TableHead
            branches={branches.branches}
            tags={branches.tags}
            currentRef={currentRef}
            defaultBranch={defaultBranch}
            path={path}
          />
          {path.length > 0 && path[0] === routeSlugs.commits ? (
            <CommitLog currentRef={currentRef} path={path} />
          ) : (
            <TableListing
              currentRef={currentRef}
              path={path}
              branches={branches.branches}
            />
          )}
        </Card>
      </Container>
    </div>
  ) : (
    <div>
      <Container style={{ marginTop: "100px" }}>
        <Card />
      </Container>
    </div>
  );
};

Explore.propTypes = {};

export default Explore;
