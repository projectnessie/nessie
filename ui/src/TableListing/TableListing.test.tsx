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
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { render, screen, waitFor } from "@testing-library/react";
import TableListing from "./TableListing";
// tslint:disable-next-line:no-implicit-dependencies
import nock from "nock";

const ShowPath = () => {
  const { pathname, search, hash } = useLocation();
  return <pre>{JSON.stringify({ pathname, search, hash })}</pre>;
};

it("TableListing renders", async () => {
  const entry = {
    name: { elements: ["a"] },
    type: "UNKNOWN",
  };
  const scope = nock("http://localhost/api/v1")
    .get("/trees/tree/main/entries?namespaceDepth=1")
    .reply(200, { token: "foo", entries: [entry] });

  const { getByText, asFragment } = render(
    <MemoryRouter initialEntries={["/tree/main"]}>
      <Routes>
        <Route path={"/tree/:branch"} element={<TableListing />} />
      </Routes>
    </MemoryRouter>
  );
  expect(asFragment()).toMatchSnapshot();
  await waitFor(() => getByText("a"));

  scope.done();
  expect(asFragment()).toMatchSnapshot();
  expect(screen.getByText("a").closest("a")).toHaveAttribute(
    "href",
    "/tree/main/a"
  );
});

it("TableListing redirects on an invalid ref", async () => {
  const scope = nock("http://localhost/api/v1")
    .get("/trees/tree/main/entries?namespaceDepth=1")
    .reply(404);

  const { getAllByText, asFragment } = render(
    <MemoryRouter initialEntries={["/tree/main"]}>
      <Routes>
        <Route path="/tree/:branch" element={<TableListing />} />
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

  scope.done();
  expect(asFragment()).toMatchSnapshot();
});
