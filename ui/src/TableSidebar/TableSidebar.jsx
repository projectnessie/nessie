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
import React from 'react';

import {authenticationService} from '../services';
import { makeStyles } from '@material-ui/core/styles';
import TreeView from '@material-ui/lab/TreeView';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import ChevronRightIcon from '@material-ui/icons/ChevronRight';
import TreeItem from '@material-ui/lab/TreeItem';
import {config} from "../config";
import {nestTables} from "../utils";

class TableSidebar extends React.Component {
  constructor(props) {
    super(props);
    const useStyles = makeStyles({
      root: {
        height: 240,
        flexGrow: 1,
        maxWidth: 400,
      },
    });
    this.state = {
      currentUser: authenticationService.currentUserValue,
      tables: null,
    };
  }

  componentDidMount() {
    console.log("mounted");
    const requestOptions = {
      method: 'GET',
      headers: {'Authorization':this.state.currentUser.token}
    };

    fetch(`${config.apiUrl}/api/v1/tables`, requestOptions)
      .then(res => {
        console.log(res);
        return res.json();
      })
      .then((data) => {
        console.log(data);
        return nestTables(data);
      })
      .then((data) => {
        this.setState({tables:data})
      })
      .catch(console.log);
  }

  render() {
    const {currentUser, tables} = this.state;
    console.log(tables);
    const renderTree = (nodes) => (
      <TreeItem key={nodes.id} nodeId={nodes.id} label={nodes.name}>
        {Array.isArray(nodes.children) ? nodes.children.map((node) => renderTree(node)) : null}
      </TreeItem>
    );

    return (
        <TreeView
          className={"tables-tree"}
          defaultCollapseIcon={<ExpandMoreIcon />}
          defaultExpandIcon={<ChevronRightIcon />}
        >
          {tables == null ? <div/> : renderTree(tables)}
        </TreeView>
      );
  }
}

export {TableSidebar};
