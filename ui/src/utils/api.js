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
    getContents(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/contents/' + params['key'] + ''
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
      return fetch(endpoint + basePath + '/api/v1/contents/' + params['key'] + '/' + params['branch'] + '/' + params['hash'] + '' + '?' + buildQuery({
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
      return fetch(endpoint + basePath + '/api/v1/contents/' + params['key'] + '/' + params['branch'] + '/' + params['hash'] + '' + '?' + buildQuery({
          'message': params['message'],
        })

        , {
          method: 'DELETE',
          headers,
          mode,
        });
    },
    setContents(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/contents/' + params['key'] + '/' + params['hash'] + '' + '?' + buildQuery({
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
      return fetch(endpoint + basePath + '/api/v1/contents/' + params['key'] + '/' + params['hash'] + '' + '?' + buildQuery({
          'message': params['message'],
        })

        , {
          method: 'DELETE',
          headers,
          mode,
        });
    },
    getContents(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/contents/' + params['key'] + '/' + params['ref'] + ''
        , {
          method: 'GET',
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
    createEmptyBranch(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/branch/' + params['branchName'] + ''
        , {
          method: 'POST',
          headers,
          mode,
        });
    },
    createNewBranch(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/branch/' + params['branchName'] + '/' + params['hash'] + ''
        , {
          method: 'POST',
          headers,
          mode,
        });
    },
    deleteBranch(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/branch/' + params['branchName'] + '/' + params['hash'] + ''
        , {
          method: 'DELETE',
          headers,
          mode,
        });
    },
    assignBranch(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/branch/' + params['branchName'] + '/' + params['oldHash'] + '/' + params['newHash'] + ''
        , {
          method: 'PUT',
          headers,
          mode,
        });
    },
    mergeRefIntoBranch(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/merge'
        , {
          method: 'PUT',
          headers,
          mode,
        });
    },
    commitMultipleOperations(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/multi/' + params['branch'] + '/' + params['hash'] + '' + '?' + buildQuery({
          'message': params['message'],
        })

        , {
          method: 'PUT',
          headers,
          mode,
        });
    },
    commitMultipleOperations(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/multi/' + params['hash'] + '' + '?' + buildQuery({
          'message': params['message'],
        })

        , {
          method: 'PUT',
          headers,
          mode,
        });
    },
    getMultipleContents(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/multi/' + params['ref'] + ''
        , {
          method: 'POST',
          headers,
          mode,
        });
    },
    createEmptyTag(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/tag/' + params['tagName'] + ''
        , {
          method: 'POST',
          headers,
          mode,
        });
    },
    createNewTag(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/tag/' + params['tagName'] + '/' + params['hash'] + ''
        , {
          method: 'POST',
          headers,
          mode,
        });
    },
    deleteTag(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/tag/' + params['tagName'] + '/' + params['hash'] + ''
        , {
          method: 'DELETE',
          headers,
          mode,
        });
    },
    assignTag(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/tag/' + params['tagName'] + '/' + params['oldHash'] + '/' + params['newHash'] + ''
        , {
          method: 'PUT',
          headers,
          mode,
        });
    },
    transplantCommitsIntoBranch(parameters) {
      const params = typeof parameters === 'undefined' ? {} : parameters;
      let headers = {

      };
      return fetch(endpoint + basePath + '/api/v1/trees/transplant' + '?' + buildQuery({
          'message': params['message'],
        })

        , {
          method: 'PUT',
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
