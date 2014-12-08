Schema = (keySchema, attributeSchema, tableName, capacityUnit)->
  @tableName = tableName
  @capacityUnit = capacityUnit

Schema::_parse_schema = (schema)->
  keys = {}
  attributes = {}
  for name,type of schema
    keys.push
      name: name
      type: type
  for key,value of schema
    if !TYPES[value]
      attributes.push



Schema::getRow = (keys)->
  queryParams = {}
  queryParams.tableName = @tableName
  queryParams.primaryKey = []
  for key,value of keys
    queryParams.primaryKey.push
      name: key

TYPES =
  INTEGER: 1
  STRING: 2
  BOOLEAN: 3
  DOUBLE: 4
  BINARY: 5

module.exports.exports = Schema