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
import {
  Navigate,
  Route,
  Routes,
  useLocation,
  useNavigate,
} from "react-router-dom";
import { NotFound } from "./NotFound";
import {
  Branch,
  LogEntry,
  LogResponse,
  Reference,
  Tag,
} from "./generated/utils/api";
import { api } from "./utils";
import { factory } from "./ConfigLog4j";
import { CommitLog } from "./CommitLog";
import { CommitBase } from "./CommitBase";
import Main from "./Main/Main";
import { CommitDetails } from "./CommitDetails";
import { TreeBase } from "./TreeBase";
import { TableListing } from "./TableListing";
import { Contents } from "./Contents";

const log = factory.getLogger("api.ExploreTree");

const fetchLog = (
  ref: string,
  maxRecords: number,
  pageToken = ""
): Promise<void | LogResponse | undefined> => {
  const params = pageToken
    ? { ref, maxRecords, pageToken }
    : { ref, maxRecords };
  return api()
    .getCommitLog(params)
    .then((data) => {
      return data;
    })
    .catch((t: Response) => {
      log.error(`CommitLog ${JSON.stringify(t)} with error ${t.status}`);
    });
};

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

const refName = (a: Reference): string => {
  const t = a.type;
  switch (t) {
    case "DETACHED":
      return a.hash;
    case "BRANCH":
      return a.name;
    case "TAG":
      return a.name;
  }
};

const btCompare = (a: Reference, b: Reference): number => {
  return refName(a).localeCompare(refName(b));
};

interface IBranchState {
  branches: Branch[];
  tags: Tag[];
}

interface Slug {
  currentRef: string;
  path: string[];
  isCommits?: boolean;
}

const parseSlug = (
  slug: string,
  branch: string,
  defaultBranch: string,
  branches: Branch[],
  tags: Tag[]
): Slug => {
  if (!branch || branch.length === 0) {
    return { currentRef: defaultBranch, path: [] };
  }

  // if we have a slug, need to figure out what portion is related to a ref versus a key.
  let sub = "";
  const checker = (b: Branch) => {
    if (branch.toLowerCase().startsWith(b.name.toLowerCase())) {
      sub = b.name ?? "";
    }
  };

  branches.forEach(checker);
  tags.forEach(checker);

  const path = slug.split("/").filter((i) => i);

  return { currentRef: sub, path };
};

const updateRef = (
  location: string,
  defaultBranch: string,
  branches: Branch[],
  tags: Tag[]
): Slug | void => {
  if (!defaultBranch || !branches) {
    return;
  }

  const locationParts = location.split("/").filter((x) => {
    return x.length > 0;
  });
  if (locationParts[0] === "notfound") {
    return;
  }
  if (locationParts.length <= 1) {
    throw new Error("invalid url");
  }
  const view = locationParts[0];

  if (view === "commits" || view === "tree" || view === "content") {
    const branch = locationParts[1];
    const slug =
      locationParts.length > 2 ? locationParts.slice(2).join("/") : "";

    return {
      ...parseSlug(slug, branch, defaultBranch, branches, tags),
      isCommits: view === "commits",
    };
  } else {
    throw new Error("invalid url");
  }
};

const App: React.FunctionComponent = () => {
  const location = useLocation();
  const history = useNavigate();
  const [path, setPath] = useState<string[]>([]);
  const [currentRef, setCurrentRef] = useState<string>();
  const [defaultBranch, setDefaultBranch] = useState<string>("main");
  const [branches, setBranches] = useState<IBranchState>({
    branches: [],
    tags: [],
  });
  const [hasMoreLog, setHasMoreLog] = useState<LogResponse["hasMore"]>(false);
  const [paginationToken, setPaginationToken] =
    useState<LogResponse["token"]>();
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [logList, setLogList] = useState<LogEntry[]>([
    {
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
    },
  ]);
  const [viewCommits, setViewCommits] = useState<boolean>(false);

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
    try {
      const newSlug = updateRef(
        location.pathname,
        defaultBranch,
        branches.branches,
        branches.tags
      );

      if (newSlug) {
        setCurrentRef(newSlug.currentRef);
        setPath(newSlug.path);
        setViewCommits(newSlug.isCommits || false);
      }
    } catch (e) {
      if (location.pathname === "/notfound" || location.pathname === "/") {
        return;
      }
      history("/notfound");
    }
  }, [location, defaultBranch, branches]);

  const fetchMoreLog = async (isResetData = false) => {
    const pageToken = isResetData ? "" : paginationToken;
    const fetchLogResults = await fetchLog(
      currentRef as string,
      rowsPerPage,
      pageToken
    );

    if (fetchLogResults) {
      const { hasMore, token } = fetchLogResults;
      const logEntries =
        fetchLogResults.logEntries && fetchLogResults.logEntries.length > 0
          ? fetchLogResults.logEntries
          : [];
      const dataList = isResetData ? logEntries : logList.concat(logEntries);
      setLogList(dataList);
      setHasMoreLog(hasMore);
      setPaginationToken(token);
    } else {
      history("/notfound");
    }
  };

  useEffect(() => {
    if (currentRef === undefined) {
      return;
    }
    if (viewCommits) {
      void fetchMoreLog(true);
    }
  }, [currentRef, rowsPerPage, viewCommits]);

  const handleChangePage = (
    event: React.MouseEvent<HTMLButtonElement> | null,
    newPage: number
  ) => {
    setPage(newPage);
    if (hasMoreLog) {
      void fetchMoreLog();
    }
  };

  const handleChangeRowsPerPage = (
    event: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  return (
    <Routes>
      <Route path={"/"} element={<Main />}>
        <Route index element={<Navigate to={`/tree/${defaultBranch}`} />} />
        <Route path="notfound" element={<NotFound />} />
        <Route
          path={"tree"}
          element={
            <TreeBase
              defaultBranch={defaultBranch}
              branches={branches.branches}
              tags={branches.tags}
              path={path}
              currentRef={currentRef}
              content={false}
            />
          }
        >
          <Route index element={<Navigate to={defaultBranch} />} />
          <Route path={":branch"} element={<TableListing />} />
          <Route path={":branch/*"} element={<TableListing />} />
          <Route path={"*"} element={<Navigate to={"/"} />} />
        </Route>
        <Route
          path={"content"}
          element={
            <TreeBase
              defaultBranch={defaultBranch}
              branches={branches.branches}
              tags={branches.tags}
              path={path}
              currentRef={currentRef}
              content
            />
          }
        >
          <Route index element={<Navigate to={"/"} />} />
          <Route path={":branch"} element={<Navigate to={"/"} />} />
          <Route path={":branch/*"} element={<Contents />} />
          <Route path={"*"} element={<Navigate to={"/"} />} />
        </Route>
        <Route
          path={"commits"}
          element={
            <CommitBase
              defaultBranch={defaultBranch}
              branches={branches.branches}
              tags={branches.tags}
              path={path}
              currentRef={currentRef}
            />
          }
        >
          <Route index element={<Navigate to={defaultBranch} />} />
          <Route
            path={":branch"}
            element={
              <CommitLog
                logList={logList}
                handleChangeRowsPerPage={handleChangeRowsPerPage}
                handleChangePage={handleChangePage}
                rowsPerPage={rowsPerPage}
                page={page}
                hasMoreLog={hasMoreLog || false}
              />
            }
          />
          <Route
            path={":branch/:commit_id"}
            element={<CommitDetails commitDetails={logList} />}
          />
          <Route path={"*"} element={<Navigate to={"/"} />} />
        </Route>
      </Route>
    </Routes>
  );
};

export { App };
