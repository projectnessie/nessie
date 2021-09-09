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
import { BrowserRouter } from "react-router-dom";
import { render, screen } from "@testing-library/react";
import TableHead from "./TableHead";
import { Branch, Tag } from "../generated/utils/api";

it("TableHead renders with path", () => {
  const { asFragment } = render(
    <BrowserRouter>
      <TableHead
        currentRef={"main"}
        path={["a", "b", "c"]}
        tags={[tag("t1"), tag("t2"), tag("t3")]}
        branches={[branch("b1"), branch("b2"), branch("b3")]}
        defaultBranch={"main"}
      />
    </BrowserRouter>
  );

  expect(asFragment()).toMatchSnapshot();
  expect(screen.getByText("c").closest("a")).toHaveAttribute(
    "href",
    "/tree/main/a/b/c"
  );
});

it("TableHead renders without path", () => {
  const { asFragment } = render(
    <BrowserRouter>
      <TableHead
        currentRef={"main"}
        tags={[tag("t1"), tag("t2"), tag("t3")]}
        branches={[tag("b1"), tag("b2"), tag("b3"), tag("b4")]}
        defaultBranch={"main"}
      />
    </BrowserRouter>
  );

  expect(asFragment()).toMatchSnapshot();
  expect(screen.getByText("3")).toBeInTheDocument();
  expect(screen.getByText("4")).toBeInTheDocument();
});

it("TableHead renders without current ref", () => {
  const { asFragment } = render(
    <BrowserRouter>
      <TableHead
        defaultBranch={"main"}
        tags={[tag("t1"), tag("t2"), tag("t3")]}
        branches={[tag("b1"), tag("b2"), tag("b3"), tag("b4")]}
      />
    </BrowserRouter>
  );

  expect(asFragment()).toMatchSnapshot();
});

it("TableHead renders with different current ref", () => {
  const { asFragment } = render(
    <BrowserRouter>
      <TableHead
        currentRef={"b2"}
        defaultBranch={"main"}
        tags={[tag("t1"), tag("t2"), tag("t3")]}
        branches={[tag("b1"), tag("b2"), tag("b3"), tag("b4")]}
      />
    </BrowserRouter>
  );

  expect(asFragment()).toMatchSnapshot();
});

const tag = (name: string, hash = "deadbeef"): Tag => {
  return { name, hash };
};

const branch = (name: string, hash = "deadbeef"): Branch => {
  return { name, hash };
};
