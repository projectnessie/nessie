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
import {config} from "../../config";

export default function CreateBranch(props) {
  return (
    <div>
      <h2>Create Branch</h2>
      <Formik
        initialValues={{
          baseBranch: props.currentBranch,
          newBranch: ''
        }}
        validationSchema={Yup.object().shape({
          newBranch: Yup.string().required('Branch name is required')
        })}
        onSubmit={({ baseBranch, newBranch }, { setStatus, setSubmitting }) => {
          setStatus();
          const requestOptions = {
            method: 'POST',
            headers: {'Authorization': props.currentUser.token, 'Content-Type': 'application/json'},
            body: JSON.stringify({'id': baseBranch, 'name': newBranch})
          };

          return fetch(`${config.apiUrl}/objects/${newBranch}/`, requestOptions)
            .then(
              props.handleClose(),
              error => {
                setSubmitting(false);
                setStatus(error);
              }
            );
        }}>
        {({ errors, status, touched, isSubmitting }) => (
          <Form>
            <div className="form-group">
              <label htmlFor="baseBranch">Base Branch Name</label>
              <Field name="baseBranch" type="text" className={'form-control' + (errors.baseBranch && touched.baseBranch ? ' is-invalid' : '')} />
              <ErrorMessage name="baseBranch" component="div" className="invalid-feedback" />
            </div>
            <div className="form-group">
              <label htmlFor="newBranch">New Branch Name</label>
              <Field name="newBranch" type="newBranch" className={'form-control' + (errors.newBranch && touched.newBranch ? ' is-invalid' : '')} />
              <ErrorMessage name="newBranch" component="div" className="invalid-feedback" />
            </div>
            <div className="form-group">
              <button type="submit" className="btn btn-primary" disabled={isSubmitting}>Create Branch</button>
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
