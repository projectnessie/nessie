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

import React from "react";
import { MemoryRouter, Routes, Route, useLocation } from "react-router-dom";
import { render, screen, waitFor } from "@testing-library/react";
import CommitLog from "./CommitLog";
import { LogEntry } from "../generated/utils/api";

const ShowPath = () => {
  const { pathname, search, hash } = useLocation();
  return <pre>{JSON.stringify({ pathname, search, hash })}</pre>;
};

it("Commit log renders", async () => {
  const now = new Date();
  now.setDate(now.getDate() - 1);
  const logEntry = {
    commitMeta: {
      hash: "deadbeef",
      author: "bob",
      commitTime: now,
      committer: "sally",
      message: "commitMessage",
      properties: { a: "b", c: "d" },
    },
  };

  const logList: LogEntry[] = [logEntry];
  const { getByText } = render(
    <React.StrictMode>
      <MemoryRouter initialEntries={["/commits/main"]}>
        <Routes>
          <Route
            path={"/commits/:branch"}
            element={
              <CommitLog
                rowsPerPage={10}
                page={0}
                handleChangePage={() => {
                  // do nothing
                }}
                handleChangeRowsPerPage={() => {
                  // do nothing
                }}
                hasMoreLog={false}
                logList={logList}
              />
            }
          />
        </Routes>
      </MemoryRouter>
    </React.StrictMode>
  );
  // disable snapshots w/ unique ids until https://github.com/facebook/jest/issues/8618 is fixed
  // expect(asFragment()).toMatchSnapshot();
  await waitFor(() => getByText("deadbeef"));

  expect(screen.getByText("deadbeef")).toBeInTheDocument();
});

it("Commit redirects on an invalid ref", async () => {
  const logList: LogEntry[] = [];
  const { getAllByText, asFragment } = render(
    <MemoryRouter initialEntries={["/commits/main"]}>
      <Routes>
        <Route
          path="/commits/:branch"
          element={
            <CommitLog
              rowsPerPage={10}
              page={0}
              handleChangePage={() => {
                // do nothing
              }}
              handleChangeRowsPerPage={() => {
                // do nothing
              }}
              hasMoreLog={false}
              logList={logList}
            />
          }
        />
        <Route path={"/notfound"} element={<ShowPath />} />
      </Routes>
    </MemoryRouter>
  );
  expect(asFragment()).toMatchSnapshot();
  await waitFor(() =>
    getAllByText((content, element) => {
      return element?.tagName.toLowerCase() === "div";
    })
  );

  expect(asFragment()).toMatchSnapshot();
});
