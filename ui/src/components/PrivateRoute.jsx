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
import { Route, Redirect } from 'react-router-dom';

import { authenticationService } from '../services';

export const PrivateRoute = ({ component: Component, currentTables, branches, ...rest }) => (
  <Route {...rest} render={props => {
    const currentUser = authenticationService.currentUserValue;
    if (!currentUser) {
      // not logged in so redirect to login page with the return url
      return <Redirect to={{ pathname: '/login', state: { from: props.location } }} />
    }
    // authorised so return component
    return <Component {...props} branches={branches} currentTables={currentTables}/>
  }} />
)
