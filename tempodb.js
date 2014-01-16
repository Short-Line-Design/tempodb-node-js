/*
 * tempodb.js
 *
 * Description  : A Node.js client for TempoDB
 *
 * Author       : Andrew J Cronk <andy@tempo-db.com>
 *
 * Notes        :
 *
 */

var http = require('http'),
    https = require('https'),
    async = require('async'),
    moment = require('moment'),
    request = require('request'),
    _ = require('underscore');
    //zlib = require('zlib');

/**
 * Expose the tempodb client
 */
module.exports = exports = TempoDBClient;
module.exports.TempoDBClient = TempoDBClient;

/**
 * Setup the tempodb client
 * - 'key'  The api key for the tempodb database
 * - 'secret'  The api secret for the tempodb database
 * - 'options'  The http connection settings for the web service
 */
function TempoDBClient(key, secret, options) {
    // Validate the key, secret, and options params
    if (!_.isString(key) || _.isEmpty(key)) {
        throw 'TempoDBClient: The key param must be a valid string!';
    }
    if (!_.isString(secret) || _.isEmpty(secret)) {
        throw 'TempoDBClient: The secret param must be a valid string!';
    }
    if (!_.isObject(options)) {
        options = {
            // host | hostname (string)
            // port (tcp port number)
            // secure (bool flag)
            // version (string)

            // timeout (number)
            // max_sockets (number)
            // max_retries (number)
        };
    }

    // Validate and set the host
    if (_.isString(options.hostname) && !_.isEmpty(options.hostname)) {
        options.host = options.hostname;
    } else if (!_.isString(options.host) || _.isEmpty(options.host)) {
        // Set the default tempodb host
        options.host = 'api.tempo-db.com';
    }

    // Validate and set the port
    if (!_.isNumber(options.port) || options.port <= 0) {
        // Set the default tempodb port
        options.port = 443;
    }

    // Validate and set the secure flag
    if (options.port === 443 || options.port === 8443 || options.secure !== false) {
        options.secure = true;
    }

    // Validate and set the api version
    if (!_.isString(options.version) || _.isEmpty(options.version)) {
        // Set the default tempodb version
        options.version = 'v1';
    }

    // Validate and set the api timeout
    if (!_.isNumber(options.timeout) || options.timeout <= 0) {
        // Set the default tempodb timeout
        options.timeout = 150000;
    }

    // Validate and set the request max_sockets
    if (!_.isNumber(options.max_sockets) || options.max_sockets <= 0) {
        // Set the default request max_sockets
        options.max_sockets = 500;
    }

    // Validate and set the request max_retries on timeout or error
    if (!_.isNumber(options.max_retries) || options.max_retries <= 0) {
        // Set the default tempodb max_retries
        options.max_retries = 0;
    }

    // Setup the basic authentication string
    // and the required http header fields
    var headers = {
        'Host': options.host,
        'Connection': 'keep-alive',
        'User-Agent': 'tempodb-nodejs',
        'Content-Type': 'application/json'
        //'Accept-Encoding': 'gzip,deflate'
    };

    // Set the instance vars
    this.key = key;
    this.secret = secret;
    this.req_headers = headers;
    this.query_options = options;
    this.process_time = {
        total: 0,
        get: 0, put: 0,
        post: 0, delete: 0
    };
};

/**
 * Setup the tempodb client http call function
 * - 'method'  The api key for the tempodb database
 * - 'query_path'  The api secret for the tempodb database
 * - 'query_params'  The http connection settings for the web service
 * - 'time_series_data'  The time series data that will be written to the web service
 */
TempoDBClient.prototype.call = function call(method, query_path, query_params, time_series_data, callback) {
    // Validate the method, query_path and query_params params
    if (!_.isString(method) || _.isEmpty(method)) {
        throw 'TempoDBClient: The method param must be a valid string!';
    }
    if (!_.isString(query_path) || _.isEmpty(query_path)) {
        throw 'TempoDBClient: The query_path param must be a valid string!';
    }
    // Set a local (in-scope)
    // copy of the instance vars
    var me = this,
        key = this.key,
        secret = this.secret,
        req_headers = this.req_headers,
        query_options = this.query_options;
        process_time = this.process_time;

    // Set the encoded query path
    // from the query path param
    var encoded_query_path = encodeURI(query_path);

    // Set the retry_count and max_retries from the query params
    var retry_count = 0, max_retries = query_options.max_retries;
    if (query_params) {
        if (_.isNumber(query_params.retry_count)) {
            retry_count = query_params.retry_count;
        }
        if (_.isNumber(query_params.max_retries)) {
            max_retries = query_params.max_retries;
        }
        delete query_params.retry_count;
        delete query_params.max_retries;
        if (_.isEmpty(query_params)) {
            query_params = undefined;
        }
    }

    // Set the encoded query params
    // from the query params param
    var encoded_query_params = '';
    if (query_params && !_.isEmpty(query_params)) {
        var query_params_array = [];
        for (var query_param in query_params) {
            var value = query_params[query_param];
            if (_.isArray(value)) {
                for (var val in value){
                    query_params_array.push(encodeURIComponent(query_param) + "=" + encodeURIComponent(value[val]));
                }
            }
            else if (_.isObject(value)) {
                for (var val in value){
                    query_params_array.push(encodeURIComponent(query_param) + "[" + encodeURIComponent(val) + "]=" + encodeURIComponent(value[val]));
                }
            }
            else {
                query_params_array.push(encodeURIComponent(query_param) + "=" + encodeURIComponent(value.toString()));
            }
        }
        encoded_query_params = '?' + query_params_array.join("&");
    };

    // Set the full path using the secure flag,
    // host, version, encoded query_path,
    // and encoded query params values
    var path = 'http' + ((query_options.secure) ? 's://' : '://') + query_options.host;
    if (query_options.secure && query_options.port !== 443) {
        path += ':' + query_options.port.toString();
    } else if (!query_options.secure && query_options.port !== 80) {
        path += ':' + query_options.port.toString();
    }
    path += '/' + query_options.version + encoded_query_path + encoded_query_params;

    // Set the request start time
    var startRequest = process.hrtime();

    // Initiate the http query and set the response callback
    return request({
        headers: req_headers,
        uri: path, method: method,
        timeout: query_options.timeout,
        auth: { user: key, pass: secret },
        followRedirect: false, maxRedirects: 0,
        pool: { maxSockets: query_options.max_sockets },

        // Set the request body and json content type
        json: (time_series_data) ? time_series_data : {}
    }, function req_callback(error, response, body) {
        // Set the request end time and secs
        var deltaRequest = process.hrtime(startRequest);
        // TODO - Fix This!  This should really be a full metric
        // including percentiles and provide a debug setting
        // so that it can be printed periodically
        process_time.total += deltaRequest[0] + (deltaRequest[1] / 1e9);
        if (method === 'GET') {
            process_time.get += deltaRequest[0] + (deltaRequest[1] / 1e9);
        } else if (method === 'PUT') {
            process_time.put += deltaRequest[0] + (deltaRequest[1] / 1e9);
        } else if (method === 'POST') {
            process_time.post += deltaRequest[0] + (deltaRequest[1] / 1e9);
        } else if (method === 'DELETE') {
            process_time.delete += deltaRequest[0] + (deltaRequest[1] / 1e9);
        }
        //if ((deltaRequest[0] + (deltaRequest[1] / 1e9)) > 5.0) {
            //console.log('TempoDBClient.req_callback:  Request completed in ' + (deltaRequest[0] + (deltaRequest[1] / 1e9)) + ' secs.')
        //}

        // Log an error (if configured) and
        // send the error to the callback
        if (error) {
            //if (query_options.log === 'warn' || query_options.log === 'error') {
                console.warn('TempoDBClient.req_callback:  Error Response - ' + path + ' - ' +
                             'status ' + ((response) ? response.statusCode : response) +
                             ' - code ' + error.code + ' - ' + error);
            //}

            // Retry the query if the retry_count is less than the max_retries
            if ((error.code === 'ETIMEDOUT' || error.code === 'ECONNRESET' || true) &&
                (!retry_count || retry_count < max_retries) &&
                 _.isNumber(max_retries) && max_retries > 0) {
                // Increment or set the retry_count
                if (!query_params) { query_params = {}; }
                if (!retry_count) { retry_count = 0; }
                query_params.retry_count = retry_count + 1;
                query_params.max_retries = max_retries;

                // Call this function recursively
                return me.call(method, query_path, query_params, time_series_data, callback);
            }
            // Return an error to the callback
            return callback(error);
        }

        // Log the response complete
        //if (response.statusCode !== 200 && query_options.log === 'debug') {
            //console.log('TempoDBClient.req_callback:  Response Status ' + response.statusCode + ' - ' + // TODO - Fix This!);
        //} else if (query_options.log === 'trace') {
            //console.log('TempoDBClient.req_callback:  Response Status ' + response.statusCode + ' - ' + // TODO - Fix This!);
        //}

        // Return and indicate success
        return callback(null, {
            response: response.statusCode,
            body: body
        });
    });
};

/**
 * Setup the tempodb client http split function
 * cascading to the http call function in parallel
 * - 'method'  The api key for the tempodb database
 * - 'query_path'  The api secret for the tempodb database
 * - 'query_params'  The http connection settings for the web service
 * - 'time_series_data'  The time series data that will be written to the web service
 */
TempoDBClient.prototype.split = function split(method, query_path, query_params, time_series_data, callback) {
    // Split the keys into multiple key sets with each
    // set containing a subset of the keys such that
    // each set stays below a 4K character length
    // assuming a 500 character max for other
    // query parameters and the url string
    var keySet = {}, keySetIndex = 1;
    for (var i = 0; i < query_params.key.length; i++) {
        // Increment the key set index if the length crosses the limit
        if (keySet['set_' + keySetIndex] && keySet['set_' + keySetIndex].len &&
            keySet['set_' + keySetIndex].len + query_params.key[i].length > 3000) {
            // Increment the key set index
            keySetIndex = keySetIndex + 1;
        }
        // Setup the key set object if needed
        if (!keySet['set_' + keySetIndex] || !keySet['set_' + keySetIndex].len) {
            keySet['set_' + keySetIndex] = { len: 0, keys: [] };
        }
        // Push the key onto the key set keys array
        keySet['set_' + keySetIndex].keys.push(query_params.key[i]);
        keySet['set_' + keySetIndex].len += query_params.key[i].length;
    }
    // Setup the set result response and body
    var me = this, setResult = { response: undefined, body: [] };
    return async.forEach(_.keys(keySet), function (set, callback) {
        // Set the query params for the query on this set of keys
        var set_query_params = _.extend({}, query_params);
        set_query_params.key = keySet[set].keys;
        // Send the 'split' query to the call method
        return me.call(method, query_path, set_query_params, time_series_data, function (error, result) {
            if (error) {
                // Errors were logged in the call method
                return callback(error);
            }
            if (!result || result.response !== 200 || !_.isArray(result.body)) {
                // TODO - Fix This!  If multiple queries fail in this
                // method, only a single response can be returned
                // meaning that we should log all responses

                // Set the set result response
                setResult.response = result.response;
                setResult.reason = result.body;
            } else {
                // Add the result body to the set result body
                setResult.body = setResult.body.concat(result.body);
            }
            return callback();
        });
    }, function (error) { // async.forEach
        if (error) {
            // Errors were logged in the call method
            return callback(error);
        }

        // Set the result response to 200 if
        // all queries completed successfully
        if (_.isUndefined(setResult.response)) {
            setResult.response = 200;
        } else {
            setResult.body = setResult.reason;
        }
        return callback(null, setResult);
    });
};

TempoDBClient.prototype.create_series = function(key, callback) {
    data = {};

    if (typeof key == 'string' && key) {
        data.key = key;
    }

    return this.call('POST', '/series/', null, data, callback);
}

TempoDBClient.prototype.get_series = function(options, callback) {
    /*
        options
            id (Array of ids or single id)
            key (Array of keys or single key)
            tag (string or Array[string])
            attr ({key: val, key2: val2})

    */
    options = options || {};
    return this.call('GET', '/series/', options, null, callback);
}

TempoDBClient.prototype.delete_series = function(options, callback) {
    /*
        options
            id (Array of ids or single id)
            key (Array of keys or single key)
            tag (string or Array[string])
            attr ({key: val, key2: val2})
            allow_truncation (Boolean)

    */
    options = options || {};
    return this.call('DELETE', '/series/', options, null, callback);
}

TempoDBClient.prototype.update_series = function(series_id, series_key, name, attributes, tags, callback) {
    // Validate the attributes and tags parameters
    if (attributes && !_.isObject(attributes)) {
        throw 'TempoDBClient: The attributes param must be an object!';
    }
    if (tags && !_.isArray(tags)) {
        throw 'TempoDBClient: The tags param must be an array!';
    }

    // Validate the series id
    if (!_.isString(series_id) || _.isEmpty(series_id)) {
        throw 'TempoDBClient: The series id param must be a valid string!';
    }

    // Setup the query parameters
    var data = {
        id: series_id,
        key: series_key,
        name: name,
        attributes: attributes,
        tags: tags
    }

    // Execute the query
    return this.call('PUT', '/series/id/' + series_id + '/', null, data, callback);
}

TempoDBClient.prototype.read = function(start, end, options, callback) {
    /*
        options
            id (Array of ids or single id)
            key (Array of keys or single key)
            interval (string)
            function (string)

    */
    options = options || {};
    options.start = _.isString(start) ? start : start.toISOString();
    options.end = _.isString(end) ? end : end.toISOString();

    // Send 'simple no-split' queries directly to the call method
    if (!options.key || !_.isArray(options.key) || options.key.length <= 1) {
        return this.call('GET', '/data/', options, null, callback);
    }
    // Send 'split' queries to the split method cascading to the call method
    return this.split('GET', '/data/', options, null, callback);
};

TempoDBClient.prototype.read_id = function(series_id, start, end, options, callback) {
    /*
        options
            interval (string)
            function (string)

    */
    options = options || {};
    options.start = _.isString(start) ? start : start.toISOString();
    options.end = _.isString(end) ? end : end.toISOString();

    return this.call('GET', '/series/id/' + series_id + '/data/', options, null, callback);
}

TempoDBClient.prototype.read_key = function(series_key, start, end, options, callback) {
    /*
        options
            interval (string)
            function (string)

    */
    options = options || {};
    options.start = _.isString(start) ? start : start.toISOString();
    options.end = _.isString(end) ? end : end.toISOString();

    return this.call('GET', '/series/key/' + series_key + '/data/', options, null, callback);
}

TempoDBClient.prototype.single_value_by_id = function(series_id, ts, options, callback) {
  options = options || {};
  options.ts = _.isString(ts) ? ts : ts.toISOString();

  return this.call('GET', '/series/id/' + series_id + '/single/', options, null, callback);
}

TempoDBClient.prototype.single_value_by_key = function(series_key, ts, options, callback) {
  options = options || {};
  options.ts = _.isString(ts) ? ts : ts.toISOString();

  return this.call('GET', '/series/key/' + series_key + '/single/', options, null, callback);
}

TempoDBClient.prototype.single_value = function(ts, options, callback) {
    /*
        options
            direction (Specify direction to search in)
            id (Array of ids or single id)
            key (Array of keys or single key)
            tag (Array of tags)
            attr (Object of attributes)

    */
    options = options || {};
    options.ts = _.isString(ts) ? ts : ts.toISOString();

    // Send 'simple no-split' queries directly to the call method
    if (!options.key || !_.isArray(options.key) || options.key.length <= 1) {
        return this.call('GET', '/single/', options, null, callback);
    }
    // Send 'split' queries to the split method cascading to the call method
    return this.split('GET', '/single/', options, null, callback);
};

TempoDBClient.prototype.write_id = function(series_id, data, callback) {
    return this.call('POST', '/series/id/' + series_id + '/data/', null, data, callback);
}

TempoDBClient.prototype.write_key = function(series_key, data, callback) {
    return this.call('POST', '/series/key/' + series_key + '/data/', null, data, callback);
}

TempoDBClient.prototype.write_bulk = function(ts, data, callback) {
    var body = {
        t: _.isString(ts) ? ts : ts.toISOString(),
        data: data
    }

    return this.call('POST', '/data/', null, body, callback);
}

TempoDBClient.prototype.write_multi = function(data, callback) {
    return this.call('POST', '/multi/', null, data, callback)
}

TempoDBClient.prototype.increment_multi = function(data, callback) {
    return this.call('POST', '/multi/increment/', null, data, callback)
}

TempoDBClient.prototype.increment_id = function(series_id, data, callback) {
    return this.call('POST', '/series/id/' + series_id + '/increment/', null, data, callback);
}

TempoDBClient.prototype.increment_key = function(series_key, data, callback) {
    return this.call('POST', '/series/key/' + series_key + '/increment/', null, data, callback);
}

TempoDBClient.prototype.increment_bulk = function(ts, data, callback) {
    var body = {
        t: _.isString(ts) ? ts : ts.toISOString(),
        data: data
    }

    return this.call('POST', '/increment/', null, body, callback);
}

TempoDBClient.prototype.delete_id = function(series_id, start, end, callback) {
  var options = {
    start: _.isString(start) ? start : start.toISOString(),
    end:   _.isString(end) ? end : end.toISOString()
  }

  return this.call('DELETE', '/series/id/'+series_id+'/data/', options, null, callback);
}

TempoDBClient.prototype.delete_key = function(series_key, start, end, callback) {
  var options = {
    start: _.isString(start) ? start : start.toISOString(),
    end:   _.isString(end) ? end : end.toISOString()
  }

  return this.call('DELETE', '/series/key/'+series_key+'/data/', options, null, callback);
}
