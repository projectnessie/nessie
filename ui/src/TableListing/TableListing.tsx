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
import { ExploreLink } from "../ExploreLink";
import { api, ContentKey, Entry } from "../utils";
import { factory } from "../ConfigLog4j";
import { useParams, useLocation } from "react-router-dom";

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
  const filter =
    path.length > 0
      ? `entry.namespace.matches('^${path.join("\\\\.")}(\\\\.|$)')`
      : undefined;
  const refSplit = ref.includes(":") ? ref.split(":")[0] : ref;
  const hashOnRef = ref.includes(":") ? ref.split(":")[1] : undefined;
  return api()
    .getEntries({
      ref: refSplit,
      namespaceDepth: path.length + 1,
      hashOnRef,
      filter,
    })
    .then((data) => {
      return data.entries?.map((e) => entryToKey(e));
    })
    .catch((e: Response) => {
      if (e.status !== 404) {
        log.error(`Entries ${JSON.stringify(e)}`);
      }
    });
};

const entryToKey = (entry: Entry): Key => {
  return {
    name: entry.name.elements[entry.name.elements.length - 1],
    type: entry.type === "UNKNOWN" ? "CONTAINER" : "TABLE",
    contentKey: entry.name,
  };
};

interface Key {
  name: string;
  type: "CONTAINER" | "TABLE";
  contentKey: ContentKey;
}

const TableListing = (): React.ReactElement => {
  const location = useLocation();
  const { branch, "*": path } = useParams<{ branch: string; "*": string }>();
  const [keys, setKeys] = useState<Key[]>([]);

  useEffect(() => {
    const keysFn = async () => {
      const fetched = await fetchKeys(branch as string, path?.split("/") || []);
      if (fetched) {
        setKeys(fetched);
      }
    };
    void keysFn();
  }, [location]);

  return (
    <Card>
      <ListGroup variant={"flush"}>
        {keys.map((key) => {
          return groupItem(key, branch as string, path?.split("/") || []);
        })}
      </ListGroup>
    </Card>
  );
};

TableListing.defaultProps = {
  currentRef: "main",
  path: [],
  branches: [],
};

export default TableListing;
