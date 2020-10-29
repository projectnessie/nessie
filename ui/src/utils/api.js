/* eslint-disable */
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
export default createApi;
function createApi(options) {
  const basePath = '';
  const endpoint = options.endpoint || '';
  const cors = !!options.cors;
  const mode = cors ? 'cors' : 'basic';
  const buildQuery = (obj) => {
    return Object.keys(obj)
      .filter(key => typeof obj[key] !== 'undefined')
      .map((key) => {
        const value = obj[key];
        if (value === undefined) {
          return '';
        }
        if (value === null) {
          return key;
        }
        if (Array.isArray(value)) {
          if (value.length) {
            return key + '=' + value.map(encodeURIComponent).join('&' + key + '=');
          } else {
            return '';
          }
        } else {
          return key + '=' + encodeURIComponent(value);
        }
      }).join('&');
    };
  return {
    getConfig(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/config'
        , {
          method: 'GET',
          headers,
          mode,
        });
    },
    getMultipleContents(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/contents' + '?' + buildQuery({
          'ref': params['ref'],
        })

        , {
          method: 'POST',
          headers,
          mode,
        });
    },
    getContents(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/contents/' + params['key'] + '' + '?' + buildQuery({
          'ref': params['ref'],
        })

        , {
          method: 'GET',
          headers,
          mode,
        });
    },
    setContents(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/contents/' + params['key'] + '' + '?' + buildQuery({
          'branch': params['branch'],
          'hash': params['hash'],
          'message': params['message'],
        })

        , {
          method: 'POST',
          headers,
          mode,
        });
    },
    deleteContents(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/contents/' + params['key'] + '' + '?' + buildQuery({
          'branch': params['branch'],
          'hash': params['hash'],
          'message': params['message'],
        })

        , {
          method: 'DELETE',
          headers,
          mode,
        });
    },
    getAllReferences(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees'
        , {
          method: 'GET',
          headers,
          mode,
        });
    },
    assignBranch(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/branch/' + params['branchName'] + '' + '?' + buildQuery({
          'expectedHash': params['expectedHash'],
        })

        , {
          method: 'PUT',
          headers,
          mode,
        });
    },
    deleteBranch(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/branch/' + params['branchName'] + '' + '?' + buildQuery({
          'expectedHash': params['expectedHash'],
        })

        , {
          method: 'DELETE',
          headers,
          mode,
        });
    },
    commitMultipleOperations(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/branch/' + params['branchName'] + '/commit' + '?' + buildQuery({
          'expectedHash': params['expectedHash'],
          'message': params['message'],
        })

        , {
          method: 'POST',
          headers,
          mode,
        });
    },
    mergeRefIntoBranch(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/branch/' + params['branchName'] + '/merge' + '?' + buildQuery({
          'expectedHash': params['expectedHash'],
        })

        , {
          method: 'POST',
          headers,
          mode,
        });
    },
    transplantCommitsIntoBranch(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/branch/' + params['branchName'] + '/transplant' + '?' + buildQuery({
          'expectedHash': params['expectedHash'],
          'message': params['message'],
        })

        , {
          method: 'POST',
          headers,
          mode,
        });
    },
    assignTag(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/tag/' + params['tagName'] + '' + '?' + buildQuery({
          'expectedHash': params['expectedHash'],
        })

        , {
          method: 'PUT',
          headers,
          mode,
        });
    },
    deleteTag(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/tag/' + params['tagName'] + '' + '?' + buildQuery({
          'expectedHash': params['expectedHash'],
        })

        , {
          method: 'DELETE',
          headers,
          mode,
        });
    },
    getDefaultBranch(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/tree'
        , {
          method: 'GET',
          headers,
          mode,
        });
    },
    createReference(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/tree'
        , {
          method: 'POST',
          headers,
          mode,
        });
    },
    getReferenceByName(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/tree/' + params['ref'] + ''
        , {
          method: 'GET',
          headers,
          mode,
        });
    },
    getEntries(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/tree/' + params['ref'] + '/entries'
        , {
          method: 'GET',
          headers,
          mode,
        });
    },
    getCommitLog(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/tree/' + params['ref'] + '/log'
        , {
          method: 'GET',
          headers,
          mode,
        });
    },

  };
}
