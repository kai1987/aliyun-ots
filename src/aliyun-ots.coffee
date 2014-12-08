_ = require 'lodash'
fs = require 'fs'
path = require 'path'

requestFn = require 'request'
utility = require 'utility'
whenjs = require 'when'
nodefn = require 'when/node'
Schema = require('protobuf').Schema

bindCallback = nodefn.bindCallback
request = nodefn.lift requestFn


schemaDesc = new Schema fs.readFileSync './ots_protocol.desc'

schemas = {}
schema = (name) ->
  if name == 'CreateTable' or name == 'DeleteTable'
    return undefined
  schemas[name] ?= schemaDesc["com.aliyun.cloudservice.ots2.#{name}"]

Client = (options)->
  @accessKeyID = options.accessKeyID
  @accessKeySecret = options.accessKeySecret
  @APIVersion = options.APIVersion || '2014-08-08'
  @instanceName = options.instanceName
  @region = options.region
  @protocol = options.protocol || 'http'

  if options.internal
    @APIHost = "#{@protocol}://#{@instanceName}.#{@region}.ots-internal.aliyuncs.com"
  else
    @APIHost = "#{@protocol}://#{@instanceName}.#{@region}.ots.aliyuncs.com"

  @requestAgent = options.agent || null
  @requestTimeout = options.requestTimeout || 5000

exports = module.exports = Client

Client::query = (action, params, cb) ->
  if _.isFunction(params)
    [params, cb] = [null, params]
  body = schema("#{action}Request").serialize params if params
  requestResult = @request action, body || ''
  bindCallback requestResult, cb

Client::createTable = (name, primaryKeys, capacityUnit, cb)->
  params =
    tableMeta:
      tableName: name
      primaryKey: primaryKeys
    reservedThroughput:
      capacityUnit: capacityUnit

  @query 'CreateTable', params, cb

Client::updateTable = (name, capacityUnit, cb)->
  params =
    tableName: name
    reservedThroughput:
      capacityUnit: capacityUnit

  @query 'UpdateTable', params, cb

Client::describeTable = (name, cb)->
  params =
    tableName: name

  @query 'DescribeTable', params, cb


Client::listTable = (cb)->
  @query 'ListTable', cb

Client::deleteTable = (name, cb)->
  @query 'DeleteTable',
    tableName: name
  , cb

Client::getRow = (name, primaryColumns, columnsToGet, cb)->
  params =
    tableName: name
    primaryKey: primaryColumns
    columnsToGet: columnsToGet

  @query 'GetRow', params, cb

Client::putRow = (tableName, condition, primaryColumns, attributeColumns, cb)->
  params =
    tableName: tableName
    condition:
      rowExistence: condition
    primaryKey: primaryColumns
    attributeColumns: attributeColumns

  @query 'PutRow', params, cb

Client::updateRow = (tableName, condition, primaryColumns, attributeColumns, cb)->
  params =
    tableName: tableName
    condition:
      rowExistence: condition
    primaryKey: primaryColumns
    attributeColumns: attributeColumns

  @query 'UpdateRow', params, cb

Client::deleteRow = (tableName, condition, primaryColumns, cb)->
  params =
    tableName: tableName
    condition:
      rowExistence: condition
    primaryKey: primaryColumns

  @query 'DeleteRow', params, cb

Client::batchGetRow = (tables, cb)->
  params =
    tables: tables

  @query 'BatchGetRow', params, cb


Client::batchWriteRow = (tables, cb)->
  params =
    tables: tables
  @query 'BatchWriteRow', params, cb


Client::getRange = (tableName, direction, columnsToGet, limit, inclusiveStartPrimaryKey, exclusiveEndPrimaryKey, cb)->
  params =
    tableName: tableName
    direction: 'FORWARD'
    columnsToGet: columnsToGet
    limit: limit
    inclusiveStartPrimaryKey: inclusiveStartPrimaryKey
    exclusiveEndPrimaryKey: exclusiveEndPrimaryKey

  @query 'GetRange', params, cb


Client::request = (opAction, body)->
  canonicalURI = '/' + opAction
  body ?= ''
  hostname = @APIHost
  headers = @_make_headers body, canonicalURI
  url = hostname + canonicalURI
  err = {}

  request
    url: url
    method: 'POST'
    headers: headers
    body: body
    json: false
    encoding: null
  .then ([res, body]) =>
    statusCode = res.statusCode
    if statusCode != 200
      err = new Error()
      try
        info = schema("Error").parse body
        err.name = info.code
        err.message = info.message
      catch
        err.name = 'requestError'
        err.message = "code=#{statusCode}"
        throw err
      throw err
    if !@_check_response_sign res, canonicalURI
      throw new Error 'WrongResponseSign'

    protoResponse = schema("#{opAction}Response")
    if !protoResponse
      return [null, res]
    try
      result = protoResponse.parse body
    catch
      result = body.toString()

    return [result, res]

  .catch (err) ->
    throw err

Client::_check_response_sign = (res, canonicalURI)->
  headers = res.headers || {}
  otsDate = new Date(headers['x-ots-date']).getTime()
  authorizationReturn = headers.authorization || ''
  sorted = []
  for key,value of headers
    if key.search('x-ots-') == 0
      sorted.push "#{key.toLowerCase().trim()}:#{value.trim()}"
  sorted.sort()
  stringToSign = "#{sorted.join('\n')}\n#{canonicalURI}"
  hmacStringToSign = utility.hmac 'sha1', @accessKeySecret, stringToSign
  authorization = "OTS #{@accessKeyID}:#{hmacStringToSign}"
  return authorization == authorizationReturn and (otsDate + 1000 * 60 * 15) > new Date().getTime()


Client::_make_headers = (body, canonicalURI)->
  bodyMD5 = utility.md5 body, 'base64'
  headers =
    'x-ots-date': new Date().toGMTString()
    'x-ots-apiversion': String @APIVersion
    'x-ots-accesskeyid': @accessKeyID
    'x-ots-instancename': @instanceName
    'x-ots-contentmd5': bodyMD5
  sorted = []
  for key,value of headers
    sorted.push "#{key}:#{value.trim()}"
  sorted.sort()
  stringToSign = "#{canonicalURI}\nPOST\n\n#{sorted.join('\n')}\n"
  headers['x-ots-signature'] = utility.hmac 'sha1', @accessKeySecret, stringToSign, 'base64'

  return headers

module.exports.createClient = (options)->
  new Client options
