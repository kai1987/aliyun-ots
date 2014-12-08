var Client, Schema, bindCallback, exports, fs, nodefn, path, request, requestFn, schema, schemaDesc, schemas, utility, whenjs, _;

_ = require('lodash');

fs = require('fs');

path = require('path');

requestFn = require('request');

utility = require('utility');

whenjs = require('when');

nodefn = require('when/node');

Schema = require('protobuf').Schema;

bindCallback = nodefn.bindCallback;

request = nodefn.lift(requestFn);

schemaDesc = new Schema(fs.readFileSync('./ots_protocol.desc'));

schemas = {};

schema = function(name) {
  if (name === 'CreateTable' || name === 'DeleteTable') {
    return void 0;
  }
  return schemas[name] != null ? schemas[name] : schemas[name] = schemaDesc["com.aliyun.cloudservice.ots2." + name];
};

Client = function(options) {
  this.accessKeyID = options.accessKeyID;
  this.accessKeySecret = options.accessKeySecret;
  this.APIVersion = options.APIVersion || '2014-08-08';
  this.instanceName = options.instanceName;
  this.region = options.region;
  this.protocol = options.protocol || 'http';
  if (options.internal) {
    this.APIHost = "" + this.protocol + "://" + this.instanceName + "." + this.region + ".ots-internal.aliyuncs.com";
  } else {
    this.APIHost = "" + this.protocol + "://" + this.instanceName + "." + this.region + ".ots.aliyuncs.com";
  }
  this.requestAgent = options.agent || null;
  return this.requestTimeout = options.requestTimeout || 5000;
};

exports = module.exports = Client;

Client.prototype.query = function(action, params, cb) {
  var body, requestResult, _ref;
  if (_.isFunction(params)) {
    _ref = [null, params], params = _ref[0], cb = _ref[1];
  }
  if (params) {
    body = schema("" + action + "Request").serialize(params);
  }
  requestResult = this.request(action, body || '');
  return bindCallback(requestResult, cb);
};

Client.prototype.createTable = function(name, primaryKeys, capacityUnit, cb) {
  var params;
  params = {
    tableMeta: {
      tableName: name,
      primaryKey: primaryKeys
    },
    reservedThroughput: {
      capacityUnit: capacityUnit
    }
  };
  return this.query('CreateTable', params, cb);
};

Client.prototype.updateTable = function(name, capacityUnit, cb) {
  var params;
  params = {
    tableName: name,
    reservedThroughput: {
      capacityUnit: capacityUnit
    }
  };
  return this.query('UpdateTable', params, cb);
};

Client.prototype.describeTable = function(name, cb) {
  var params;
  params = {
    tableName: name
  };
  return this.query('DescribeTable', params, cb);
};

Client.prototype.listTable = function(cb) {
  return this.query('ListTable', cb);
};

Client.prototype.deleteTable = function(name, cb) {
  return this.query('DeleteTable', {
    tableName: name
  }, cb);
};

Client.prototype.getRow = function(name, primaryColumns, columnsToGet, cb) {
  var params;
  params = {
    tableName: name,
    primaryKey: primaryColumns,
    columnsToGet: columnsToGet
  };
  return this.query('GetRow', params, cb);
};

Client.prototype.putRow = function(tableName, condition, primaryColumns, attributeColumns, cb) {
  var params;
  params = {
    tableName: tableName,
    condition: {
      rowExistence: condition
    },
    primaryKey: primaryColumns,
    attributeColumns: attributeColumns
  };
  return this.query('PutRow', params, cb);
};

Client.prototype.updateRow = function(tableName, condition, primaryColumns, attributeColumns, cb) {
  var params;
  params = {
    tableName: tableName,
    condition: {
      rowExistence: condition
    },
    primaryKey: primaryColumns,
    attributeColumns: attributeColumns
  };
  return this.query('UpdateRow', params, cb);
};

Client.prototype.deleteRow = function(tableName, condition, primaryColumns, cb) {
  var params;
  params = {
    tableName: tableName,
    condition: {
      rowExistence: condition
    },
    primaryKey: primaryColumns
  };
  return this.query('DeleteRow', params, cb);
};

Client.prototype.batchGetRow = function(tables, cb) {
  var params;
  params = {
    tables: tables
  };
  return this.query('BatchGetRow', params, cb);
};

Client.prototype.batchWriteRow = function(tables, cb) {
  var params;
  params = {
    tables: tables
  };
  return this.query('BatchWriteRow', params, cb);
};

Client.prototype.getRange = function(tableName, direction, columnsToGet, limit, inclusiveStartPrimaryKey, exclusiveEndPrimaryKey, cb) {
  var params;
  params = {
    tableName: tableName,
    direction: 'FORWARD',
    columnsToGet: columnsToGet,
    limit: limit,
    inclusiveStartPrimaryKey: inclusiveStartPrimaryKey,
    exclusiveEndPrimaryKey: exclusiveEndPrimaryKey
  };
  return this.query('GetRange', params, cb);
};

Client.prototype.request = function(opAction, body) {
  var canonicalURI, err, headers, hostname, url;
  canonicalURI = '/' + opAction;
  if (body == null) {
    body = '';
  }
  hostname = this.APIHost;
  headers = this._make_headers(body, canonicalURI);
  url = hostname + canonicalURI;
  err = {};
  return request({
    url: url,
    method: 'POST',
    headers: headers,
    body: body,
    json: false,
    encoding: null
  }).then((function(_this) {
    return function(_arg) {
      var body, info, protoResponse, res, result, statusCode;
      res = _arg[0], body = _arg[1];
      statusCode = res.statusCode;
      if (statusCode !== 200) {
        err = new Error();
        try {
          info = schema("Error").parse(body);
          err.name = info.code;
          err.message = info.message;
        } catch (_error) {
          err.name = 'requestError';
          err.message = "code=" + statusCode;
          throw err;
        }
        throw err;
      }
      if (!_this._check_response_sign(res, canonicalURI)) {
        throw new Error('WrongResponseSign');
      }
      protoResponse = schema("" + opAction + "Response");
      if (!protoResponse) {
        return [null, res];
      }
      try {
        result = protoResponse.parse(body);
      } catch (_error) {
        result = body.toString();
      }
      return [result, res];
    };
  })(this))["catch"](function(err) {
    throw err;
  });
};

Client.prototype._check_response_sign = function(res, canonicalURI) {
  var authorization, authorizationReturn, headers, hmacStringToSign, key, otsDate, sorted, stringToSign, value;
  headers = res.headers || {};
  otsDate = new Date(headers['x-ots-date']).getTime();
  authorizationReturn = headers.authorization || '';
  sorted = [];
  for (key in headers) {
    value = headers[key];
    if (key.search('x-ots-') === 0) {
      sorted.push("" + (key.toLowerCase().trim()) + ":" + (value.trim()));
    }
  }
  sorted.sort();
  stringToSign = "" + (sorted.join('\n')) + "\n" + canonicalURI;
  hmacStringToSign = utility.hmac('sha1', this.accessKeySecret, stringToSign);
  authorization = "OTS " + this.accessKeyID + ":" + hmacStringToSign;
  return authorization === authorizationReturn && (otsDate + 1000 * 60 * 15) > new Date().getTime();
};

Client.prototype._make_headers = function(body, canonicalURI) {
  var bodyMD5, headers, key, sorted, stringToSign, value;
  bodyMD5 = utility.md5(body, 'base64');
  headers = {
    'x-ots-date': new Date().toGMTString(),
    'x-ots-apiversion': String(this.APIVersion),
    'x-ots-accesskeyid': this.accessKeyID,
    'x-ots-instancename': this.instanceName,
    'x-ots-contentmd5': bodyMD5
  };
  sorted = [];
  for (key in headers) {
    value = headers[key];
    sorted.push("" + key + ":" + (value.trim()));
  }
  sorted.sort();
  stringToSign = "" + canonicalURI + "\nPOST\n\n" + (sorted.join('\n')) + "\n";
  headers['x-ots-signature'] = utility.hmac('sha1', this.accessKeySecret, stringToSign, 'base64');
  return headers;
};

module.exports.createClient = function(options) {
  return new Client(options);
};
