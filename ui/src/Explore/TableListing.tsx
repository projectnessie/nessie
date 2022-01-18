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
import {
  api,
  Branch,
  Content,
  ContentKey,
  Entry,
  GetMultipleContentsRequest,
  GetMultipleContentsResponse,
} from "../utils";
import { factory } from "../ConfigLog4j";
import { ContentView, EmptyMessageView } from "./Components";
import { useHistory, Redirect, useLocation } from "react-router-dom";
import { routeSlugs } from "./Constants";
import { Location, History } from "history";

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
  path: string[],
  showContent: boolean
): Promise<void | Key[] | undefined> => {
  const newPath = showContent ? Array(path[0]) : path;
  return api()
    .getEntries({
      ref,
      namespaceDepth: newPath.length + 1,
      filter: `entry.namespace.matches('${newPath.join("\\\\.")}(\\\\.|$)')`,
    })
    .then((data) => {
      return data.entries?.map((e) => entryToKey(e));
    })
    .catch((e) => log.error("Entries", e as undefined));
};

const entryToKey = (entry: Entry): Key => {
  return {
    name: entry.name.elements[entry.name.elements.length - 1],
    type: entry.type === "UNKNOWN" ? "CONTAINER" : "TABLE",
    contentKey: entry.name,
  };
};

const fetchContent = (
  ref: string,
  hashOnRef: string | undefined,
  getMultipleContentsRequest: GetMultipleContentsRequest
): Promise<void | GetMultipleContentsResponse | undefined> => {
  return api()
    .getMultipleContents({
      ref,
      hashOnRef,
      getMultipleContentsRequest,
    })
    .then((data) => {
      return data;
    })
    .catch((e) => log.error("getMultipleContents", e as undefined));
};

interface Key {
  name: string;
  type: "CONTAINER" | "TABLE";
  contentKey: ContentKey;
}

interface ITableListing {
  currentRef: string;
  path: string[];
  branches: Branch[];
}

const TableListing = ({
  currentRef,
  path,
  branches,
}: ITableListing): React.ReactElement => {
  const [keys, setKeys] = useState<Key[]>([]);
  const [isRefNotFound, setRefNotFound] = useState(false);
  const [showContent, setShowContent] = useState(false);
  const [content, setcontent] = useState<Content>();
  const location = useLocation() as Location;
  const history = useHistory() as History;
  useEffect(() => {
    const modifiedPath = location.pathname && location.pathname.split("/")[1];
    const isContentPath = modifiedPath === routeSlugs.content;
    const keysFn = async () => {
      const fetched = await fetchKeys(currentRef, path, isContentPath);
      if (fetched) {
        setKeys(fetched);
      }
      setRefNotFound(fetched === undefined);
    };
    if (keys?.length === 0 || !isContentPath) void keysFn();
    setShowContent(isContentPath);
  }, [currentRef, path]);

  useEffect(() => {
    const getContent = async () => {
      const currentBranchDetail = branches.find((branch) => {
        return branch.name === currentRef;
      });
      if (keys?.length > 0) {
        const keyObj = keys[0].contentKey;
        const contentData = await fetchContent(
          currentRef,
          currentBranchDetail?.hash,
          { requestedKeys: [keyObj] }
        );
        const contentList =
          contentData?.contents && contentData.contents.length > 0
            ? contentData.contents
            : [];
        const contentDetails =
          contentList.length > 0 ? contentList[0].content : undefined;
        setcontent(contentDetails);
      }
    };
    if (showContent) {
      void getContent();
    }
  }, [showContent, keys]);

  useEffect(() => {
    if (showContent) {
      const listPath = location.pathname.split("/", 3).join("/");
      history.push(listPath);
    }
  }, [currentRef]);

  return isRefNotFound ? (
    <Redirect to="/notfound" />
  ) : (
    <Card>
      {!showContent ? (
        <ListGroup variant={"flush"}>
          {keys.map((key) => {
            return groupItem(key, currentRef, path);
          })}
        </ListGroup>
      ) : content ? (
        <ContentView tableContent={content} />
      ) : (
        <EmptyMessageView />
      )}
    </Card>
  );
};

TableListing.defaultProps = {
  currentRef: "main",
  path: [],
  branches: [],
};

export default TableListing;
