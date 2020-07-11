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
import {ErrorMessage, Field, Form, Formik} from "formik";
import * as Yup from "yup";
import {config} from "../config";

function getBranch(token, baseBranch) {
  const requestOptions = {
    method: 'GET',
    headers: {'Authorization': token}
  };

  return fetch(`${config.apiUrl}/objects/${baseBranch}`, requestOptions)
    .then(res => res.text())
    .then(txt => JSON.parse(txt))
    .then(branch => branch.id)
}

function mergeBranches(token, baseBranch, mergeBranch, force, id, handleClose, handleError) {
  const requestOptions = {
    method: 'PUT',
    headers: {'Authorization': token, 'If-Match': '"' + id + '"'}
  };

  return fetch(`${config.apiUrl}/objects/${baseBranch}/promote?promote=${mergeBranch}&force=${force}`, requestOptions)
    .then(
      handleClose(),
      error => handleError(error)
    );
}

export default function CreateBranch(props) {
  return (
    <div>
      <h2>Merge: Commits from merge branch are merged onto base branch</h2>
      <Formik
        initialValues={{
          baseBranch: props.currentBranch,
          mergeBranch: '',
          force: false
        }}
        validationSchema={Yup.object().shape({
          mergeBranch: Yup.string().required('Merge Branch name is required')
        })}
        onSubmit={({ baseBranch, mergeBranch, force }, { setStatus, setSubmitting }) => {
          setStatus();
          return getBranch(props.currentUser.token, baseBranch)
            .then(id => mergeBranches(props.currentUser.token, baseBranch, mergeBranch, force, id, props.handleClose, error => {
              setSubmitting(false);
              setStatus(error);
            }));
        }}>
        {({ errors, status, touched, isSubmitting }) => (
          <Form>
            <div className="form-group">
              <label htmlFor="baseBranch">Base Branch Name</label>
              <Field name="baseBranch" type="text" className={'form-control' + (errors.baseBranch && touched.baseBranch ? ' is-invalid' : '')} />
              <ErrorMessage name="baseBranch" component="div" className="invalid-feedback" />
            </div>
            <div className="form-group">
              <label htmlFor="mergeBranch">Merge Branch Name</label>
              <Field name="mergeBranch" type="mergeBranch" className={'form-control' + (errors.mergeBranch && touched.mergeBranch ? ' is-invalid' : '')} />
              <ErrorMessage name="mergeBranch" component="div" className="invalid-feedback" />
            </div>
            <div className="form-group">
              <label htmlFor="force">Force Commit (may result in data loss)</label>
              <Field name="force" type="checkbox" className={'form-check' + (errors.force && touched.force ? ' is-invalid' : '')} />
              <ErrorMessage name="force" component="div" className="invalid-feedback" />
            </div>
            <div className="form-group">
              <button type="submit" className="btn btn-primary" disabled={isSubmitting}>Merge Branch</button>
              {isSubmitting &&
              <img alt="" src="/spinner.gif" />
              }
            </div>
            {status &&
            <div className={'alert alert-danger'}>{status}</div>
            }
          </Form>
        )}
      </Formik>
    </div>
  )
}
