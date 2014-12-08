require 'coffee-errors'


chai = require 'chai'
sinon = require 'sinon'

# using compiled JavaScript file here to be sure module works
ots = require '../lib/aliyun-ots.js'
config = require '../lib/config'

should = chai.should()
chai.use require 'sinon-chai'

describe '#test client', ->
  options =
    accessKeyID: config.accessKeyID
    accessKeySecret: config.accessKeySecret
    instanceName: config.instanceName
    region: config.region
    internal: config.internal
  client = ots.createClient options


  describe '#createTable', ->
    it 'should create [test1] table success', (done)->
      primaryKeys = [
        name: 'CardID'
        type: 'STRING'
      ]
      client.createTable 'test1', primaryKeys,
        read: 101
        write: 102
      .done ([result, res])->
        res.statusCode.should.equal 200
        done()
      , (err)->
        console.log err
        done err


  describe '#updateTable', ->
    it 'should update [test1] table success', (done)->
      client.updateTable 'test1',
        read: 200
        write: 200
      .done ([result, res])->
        res.statusCode.should.equal 200
      , (err)->
        console.log err
        done new Error(err)


  describe '#describeTable', ->
    it 'should get descibe info', (done)->
      client.describeTable 'test1'
      .done ([result, res])->
        console.log result.tableMeta.primaryKey[0]
        res.statusCode.should.equal 200
        result.tableMeta.tableName.should.equal 'test1'
        result.tableMeta.primaryKey[0].name.should.equal 'CardID'
        done()
      , (err)->
        console.log err
        done err


  describe '#listTable', ->
    it 'should get the table list', (done)->
      client.listTable()
      .done ([result, res])->
        res.statusCode.should.equal 200

        done()
      , (err)->
        done err


  describe '#deleteTable', ->
    it 'should delete the table success', (done)->
      client.deleteTable 'test1'
      .done ([result, res])->
        res.statusCode.should.equal 200
        done()
      , (err)->
        done err


  describe '#getRow', ->
    it 'should get the rows info success', (done)->
      primary_key_columns = [
        name: 'CardID'
        value:
          type: 'STRING'
          vString: '1'
      ]
      columns_to_get = null
      client.getRow 'test000', primary_key_columns, columns_to_get
      .done ([result, res])->
        console.log result.row.attributeColumns
        res.statusCode.should.equal 200
        done()
      , (err)->
        console.log err.body
        done err


  describe '#putRow', ->
    it 'should put row success', (done)->
      primary_key_columns = [
        name: 'CardID'
        value:
          type: 'STRING'
          vString: '1'
      ]
      attribute_columns = [
        {
          name: 'Amount'
          value:
            type: 'DOUBLE'
            vDouble: 2.5
        }
        {
          name: 'Remarks'
          value:
            type: 'STRING'
            vString: 'ice cream'
        }
      ]
      client.putRow 'test000', 'IGNORE', primary_key_columns, attribute_columns
      .done ([result, res])->
        res.statusCode.should.equal 200
        done()
      , (err)->
        console.log err
        done err


  describe '#updateRow', ->
    it 'should update row sucess', (done)->
      primary_key_columns = [
        name: 'CardID'
        value:
          type: 'STRING'
          vString: '1'
      ]
      attribute_columns = [
        {
          type: 1
          name: 'Amount'
          value:
            type: 'DOUBLE'
            vDouble: 3.5
        }
#        {
#          type: 'DELETE'
#          name: 'Remarks'
#        }
      ]

      client.putRow 'test000', 'IGNORE', primary_key_columns, attribute_columns
      .done ([result, res])->
        res.statusCode.should.equal 200
        result.consumed.capacityUnit.write.should.equal 2
        done()
      , (err)->
        console.log err
        done err


  describe '#deleteRow', ->
    it 'should delete row success', (done)->
      primary_key_columns = [
        name: 'CardID'
        value:
          type: 'STRING'
          vString: '1'
      ]
      client.deleteRow 'test000', 'IGNORE', primary_key_columns
      .done ([result, res])->
        res.statusCode.should.equal 200
        done()
      , (err)->
        console.log err
        done err


  describe '#batchGetRow', ->
    it 'should batchGetRow success', (done)->
      tables = [
        tableName: 'test000'
        rows:
          primaryKey:
            name: 'CardID'
            value:
              type: 'STRING'
              vString: '1'
        columnsToGet: [
          'CardID'
          'Remarks'
          'Amount'
        ]
      ]
      client.batchGetRow tables
      .done ([result, res])->
        res.statusCode.should.equal 200
        done()
      , (err)->
        console.log err.body
        done err


  describe '#batchWriteRow', ->
    it 'should batchWriteRow success', (done)->
      tables = [
        tableName: 'test000'
        putRows:[
          condition:
            rowExistence: 'IGNORE'
          primaryKey: [
            {
              name: 'CardID'
              value:
                type: 'STRING'
                vString: '1'
            }
          ]
          attributeColumns: [
            {
              name: 'Remarks'
              value:
                type: 'STRING'
                vString: 'remark1'
            }
            {
              name: 'Amount'
              value:
                type: 'DOUBLE'
                vDouble: 4.4
            }
          ]
        ]
      ]

      client.batchWriteRow tables
      .done ([result,res])->
        res.statusCode.should.equal 200
        done()
      , (err)->
        console.log err
        done err










