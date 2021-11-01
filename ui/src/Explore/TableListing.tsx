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
import { Card, ListGroup, ListGroupItem } from "react-bootstrap";
import InsertDriveFileOutlinedIcon from "@material-ui/icons/InsertDriveFileOutlined";
import FolderIcon from "@material-ui/icons/Folder";
import ExploreLink from "./ExploreLink";
import { api, Entry } from "../utils";
import { factory } from "../ConfigLog4j";
import { Redirect } from "react-router-dom";

const log = factory.getLogger("api.TableListing");

const groupItem = (
  key: Key,
  ref: string,
  path: string[]
): React.ReactElement => {
  const icon =
    key.type === "CONTAINER" ? <FolderIcon /> : <InsertDriveFileOutlinedIcon />;
  return (
    <ListGroupItem key={key.name}>
      <ExploreLink
        toRef={ref}
        path={path.concat(key.name)}
        type={key.type === "CONTAINER" ? "CONTAINER" : "OBJECT"}
      >
        {icon}
        {key.name}
      </ExploreLink>
    </ListGroupItem>
  );
};

const fetchKeys = (
  ref: string,
  path: string[]
): Promise<void | Key[] | undefined> => {
  return api()
    .getEntries({
      ref,
      namespaceDepth: path.length + 1,
      queryExpression: `entry.namespace.matches('${path.join(
        "\\\\."
      )}(\\\\.|$)')`,
    })
    .then((data) => {
      return data.entries?.map((e) => entryToKey(e));
    })
    .catch((e) => log.error("Entries", e));
};

const entryToKey = (entry: Entry): Key => {
  return {
    name: entry.name.elements[entry.name.elements.length - 1],
    type: entry.type === "UNKNOWN" ? "CONTAINER" : "TABLE",
  };
};

interface Key {
  name: string;
  type: "CONTAINER" | "TABLE";
}

interface ITableListing {
  currentRef: string;
  path: string[];
}

const TableListing = ({
  currentRef,
  path,
}: ITableListing): React.ReactElement => {
  const [keys, setKeys] = useState<Key[]>([]);
  const [isRefNotFound, setRefNotFound] = useState(false);
  useEffect(() => {
    const keysFn = async () => {
      const fetched = await fetchKeys(currentRef, path);
      if (fetched) {
        setKeys(fetched);
        setRefNotFound(false);
      } else {
        setRefNotFound(true);
      }
    };
    void keysFn();
  }, [currentRef, path]);
  return isRefNotFound ? (
    <Redirect to="/notfound" />
  ) : (
    <Card>
      <ListGroup variant={"flush"}>
        {keys.map((key: Key) => {
          return groupItem(key, currentRef, path);
        })}
      </ListGroup>
    </Card>
  );
};

TableListing.defaultProps = {
  currentRef: "main",
  path: [],
};

export default TableListing;
