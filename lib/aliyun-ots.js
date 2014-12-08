var BatchGetRowRequest, BatchGetRowResponse, BatchWriteRowRequest, BatchWriteRowResponse, Client, ColumnUpdate, CreateTableRequest, CreateTableResponse, DeleteRowInBatchWriteRowRequest, DeleteRowRequest, DeleteRowResponse, DeleteTableRequest, DescribeTableRequest, DescribeTableResponse, ErrorMessage, GetRangeRequest, GetRangeResponse, GetRowRequest, GetRowResponse, ListTableResponse, PutRowInBatchWriteRowRequest, PutRowRequest, PutRowResponse, ResponseMap, RowInBatchGetRowRequest, RowInBatchGetRowResponse, RowInBatchWriteRowResponse, SchemaProtobuf, TableInBatchGetRowRequest, TableInBatchGetRowResponse, TableInBatchWriteRowRequest, TableInBatchWriteRowResponse, UpdateRowInBatchWriteRowRequest, UpdateRowRequest, UpdateRowResponse, UpdateTableRequest, UpdateTableResponse, bindCallback, exports, fs, nodefn, path, request, requestFn, schemaProtobuf, utility, whenjs;

fs = require('fs');

path = require('path');

requestFn = require('request');

utility = require('utility');

whenjs = require('when');

nodefn = require('when/node');

bindCallback = nodefn.bindCallback;

SchemaProtobuf = require('protobuf').Schema;

request = nodefn.lift(requestFn);

schemaProtobuf = new SchemaProtobuf(fs.readFileSync('./ots_protocol.desc'));

ErrorMessage = schemaProtobuf['com.aliyun.cloudservice.ots2.ErrorMessage'];

CreateTableRequest = schemaProtobuf['com.aliyun.cloudservice.ots2.CreateTableRequest'];

CreateTableResponse = schemaProtobuf['com.aliyun.cloudservice.ots2.CreateTableResponse'];

UpdateTableRequest = schemaProtobuf['com.aliyun.cloudservice.ots2.UpdateTableRequest'];

UpdateTableResponse = schemaProtobuf['com.aliyun.cloudservice.ots2.UpdateTableResponse'];

DescribeTableRequest = schemaProtobuf['com.aliyun.cloudservice.ots2.DescribeTableRequest'];

DescribeTableResponse = schemaProtobuf['com.aliyun.cloudservice.ots2.DescribeTableResponse'];

ListTableResponse = schemaProtobuf['com.aliyun.cloudservice.ots2.ListTableResponse'];

DeleteTableRequest = schemaProtobuf['com.aliyun.cloudservice.ots2.DeleteTableRequest'];

CreateTableResponse = schemaProtobuf['com.aliyun.cloudservice.ots2.CreateTableResponse '];

GetRowRequest = schemaProtobuf['com.aliyun.cloudservice.ots2.GetRowRequest'];

GetRowResponse = schemaProtobuf['com.aliyun.cloudservice.ots2.GetRowResponse'];

ColumnUpdate = schemaProtobuf['com.aliyun.cloudservice.ots2.ColumnUpdate'];

UpdateRowRequest = schemaProtobuf['com.aliyun.cloudservice.ots2.UpdateRowRequest'];

UpdateRowResponse = schemaProtobuf['com.aliyun.cloudservice.ots2.UpdateRowResponse'];

PutRowRequest = schemaProtobuf['com.aliyun.cloudservice.ots2.PutRowRequest'];

PutRowResponse = schemaProtobuf['com.aliyun.cloudservice.ots2.PutRowResponse'];

DeleteRowRequest = schemaProtobuf['com.aliyun.cloudservice.ots2.DeleteRowRequest'];

DeleteRowResponse = schemaProtobuf['com.aliyun.cloudservice.ots2.DeleteRowResponse'];

RowInBatchGetRowRequest = schemaProtobuf['com.aliyun.cloudservice.ots2.RowInBatchGetRowRequest'];

TableInBatchGetRowRequest = schemaProtobuf['com.aliyun.cloudservice.ots2.TableInBatchGetRowRequest'];

BatchGetRowRequest = schemaProtobuf['com.aliyun.cloudservice.ots2.BatchGetRowRequest'];

RowInBatchGetRowResponse = schemaProtobuf['com.aliyun.cloudservice.ots2.RowInBatchGetRowResponse'];

TableInBatchGetRowResponse = schemaProtobuf['com.aliyun.cloudservice.ots2.TableInBatchGetRowResponse'];

BatchGetRowResponse = schemaProtobuf['com.aliyun.cloudservice.ots2.BatchGetRowResponse'];

PutRowInBatchWriteRowRequest = schemaProtobuf['com.aliyun.cloudservice.ots2.PutRowInBatchWriteRowRequest'];

UpdateRowInBatchWriteRowRequest = schemaProtobuf['com.aliyun.cloudservice.ots2.UpdateRowInBatchWriteRowRequest'];

DeleteRowInBatchWriteRowRequest = schemaProtobuf['com.aliyun.cloudservice.ots2.DeleteRowInBatchWriteRowRequest'];

TableInBatchWriteRowRequest = schemaProtobuf['com.aliyun.cloudservice.ots2.TableInBatchWriteRowRequest'];

BatchWriteRowRequest = schemaProtobuf['com.aliyun.cloudservice.ots2.BatchWriteRowRequest'];

RowInBatchWriteRowResponse = schemaProtobuf['com.aliyun.cloudservice.ots2.RowInBatchWriteRowResponse'];

TableInBatchWriteRowResponse = schemaProtobuf['com.aliyun.cloudservice.ots2.TableInBatchWriteRowResponse'];

BatchWriteRowResponse = schemaProtobuf['com.aliyun.cloudservice.ots2.BatchWriteRowResponse'];

GetRangeRequest = schemaProtobuf['com.aliyun.cloudservice.ots2.GetRangeRequest'];

GetRangeResponse = schemaProtobuf['com.aliyun.cloudservice.ots2.GetRangeResponse'];

ResponseMap = {
  CreateTable: CreateTableResponse,
  GetRow: GetRowResponse,
  PutRow: PutRowResponse,
  UpdateRow: UpdateRowResponse,
  DeleteRow: DeleteRowResponse,
  GetRange: GetRangeResponse,
  BatchGetRow: BatchGetRowResponse,
  BatchWriteRow: BatchWriteRowResponse,
  ListTable: ListTableResponse,
  UpdateTable: UpdateTableResponse,
  DescribeTable: DescribeTableResponse
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
  this.requestTimeout = options.requestTimeout || 5000;
};

exports = module.exports = Client;

Client.prototype.createTable = function(name, primaryKeys, capacityUnit, cb) {
  var body, params, requestResult;
  params = {
    tableMeta: {
      tableName: name,
      primaryKey: primaryKeys
    },
    reservedThroughput: {
      capacityUnit: capacityUnit
    }
  };
  body = CreateTableRequest.serialize(params);
  requestResult = this.request('CreateTable', body);
  return bindCallback(requestResult, cb);
};

Client.prototype.updateTable = function(name, capacityUnit, cb) {
  var body, params, requestResult;
  params = {
    tableName: name,
    reservedThroughput: {
      capacityUnit: capacityUnit
    }
  };
  body = UpdateTableRequest.serialize(params);
  requestResult = this.request('UpdateTable', body);
  return bindCallback(requestResult, cb);
};

Client.prototype.describeTable = function(name, cb) {
  var body, params, requestResult;
  params = {
    tableName: name
  };
  body = DescribeTableRequest.serialize(params);
  requestResult = this.request('DescribeTable', body);
  return bindCallback(requestResult, cb);
};

Client.prototype.listTable = function(cb) {
  var requestResult;
  requestResult = this.request('ListTable', '');
  return bindCallback(requestResult, cb);
};

Client.prototype.deleteTable = function(name, cb) {
  var body, params, requestResult;
  params = {
    tableName: name
  };
  body = DeleteTableRequest.serialize(params);
  requestResult = this.request('DeleteTable', body);
  return bindCallback(requestResult, cb);
};

Client.prototype.getRow = function(name, primaryColumns, columnsToGet, cb) {
  var body, params, requestResult;
  params = {
    tableName: name,
    primaryKey: primaryColumns,
    columnsToGet: columnsToGet
  };
  body = GetRowRequest.serialize(params);
  requestResult = this.request('GetRow', body);
  return bindCallback(requestResult, cb);
};

Client.prototype.putRow = function(tableName, condition, primaryColumns, attributeColumns, cb) {
  var body, params, requestResult;
  params = {
    tableName: tableName,
    condition: {
      rowExistence: condition
    },
    primaryKey: primaryColumns,
    attributeColumns: attributeColumns
  };
  body = PutRowRequest.serialize(params);
  requestResult = this.request('PutRow', body);
  return bindCallback(requestResult, cb);
};

Client.prototype.updateRow = function(tableName, condition, primaryColumns, attributeColumns, cb) {
  var body, params, requestResult;
  params = {
    tableName: tableName,
    condition: {
      rowExistence: condition
    },
    primaryKey: primaryColumns,
    attributeColumns: attributeColumns
  };
  body = UpdateRowRequest.serialize(params);
  requestResult = this.request('UpdateRow', body);
  return bindCallback(requestResult, cb);
};

Client.prototype.deleteRow = function(tableName, condition, primaryColumns, cb) {
  var body, params, requestResult;
  params = {
    tableName: tableName,
    condition: {
      rowExistence: condition
    },
    primaryKey: primaryColumns
  };
  body = DeleteRowRequest.serialize(params);
  requestResult = this.request('DeleteRow', body);
  return bindCallback(requestResult, cb);
};

Client.prototype.batchGetRow = function(tables, cb) {
  var body, params, requestResult;
  params = {
    tables: tables
  };
  body = BatchGetRowRequest.serialize(params);
  requestResult = this.request('BatchGetRow', body);
  return bindCallback(requestResult, cb);
};

Client.prototype.batchWriteRow = function(tables, cb) {
  var body, params, requestResult;
  params = {
    tables: tables
  };
  body = BatchWriteRowRequest.serialize(params);
  requestResult = this.request('BatchWriteRow', body);
  return bindCallback(requestResult, cb);
};

Client.prototype.getRange = function(tableName, direction, columnsToGet, limit, inclusiveStartPrimaryKey, exclusiveEndPrimaryKey, cb) {
  var body, params, requestResult;
  params = {
    tableName: tableName,
    direction: 'FORWARD',
    columnsToGet: columnsToGet,
    limit: limit,
    inclusiveStartPrimaryKey: inclusiveStartPrimaryKey,
    exclusiveEndPrimaryKey: exclusiveEndPrimaryKey
  };
  body = GetRangeRequest.serialize(params);
  requestResult = this.request('GetRange', body);
  return bindCallback(requestResult, cb);
};

Client.prototype.request = function(opAction, body) {
  var canonicalURI, err, headers, hostname, self, url;
  canonicalURI = '/' + opAction;
  self = this;
  if (body == null) {
    body = '';
  }
  hostname = this.APIHost;
  headers = self._make_headers(body, canonicalURI);
  url = hostname + canonicalURI;
  err = {};
  return request({
    url: url,
    method: 'POST',
    headers: headers,
    body: body,
    json: false,
    encoding: null
  }).then(function(_arg) {
    var body, e, protoResponse, res, result, statusCode;
    res = _arg[0], body = _arg[1];
    statusCode = res.statusCode;
    if (!self._check_response_sign(res, canonicalURI)) {
      throw 'WrongResponseSign';
    }
    if (statusCode !== 200) {
      err = new Error('requestError');
      err.name = 'requestError';
      err.code = statusCode;
      try {
        err.info = ErrorMessage.parse(body);
      } catch (_error) {
        e = _error;
        err.info = 'MalformedError';
      }
      try {
        err.body = body.toString();
      } catch (_error) {
        err.body = null;
      }
      throw err;
    }
    protoResponse = ResponseMap[opAction];
    if (!protoResponse) {
      return [null, res];
    }
    try {
      result = protoResponse.parse(res.body);
    } catch (_error) {
      e = _error;
      err = new Error('malFormaedBuf');
      err.body = body.toString();
      err.info = e.message;
      throw err;
    }
    return [result, res];
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
