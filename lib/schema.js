var Schema, TYPES;

Schema = function(keySchema, attributeSchema, tableName, capacityUnit) {
  this.tableName = tableName;
  return this.capacityUnit = capacityUnit;
};

Schema.prototype._parse_schema = function(schema) {
  var attributes, key, keys, name, type, value, _results;
  keys = {};
  attributes = {};
  for (name in schema) {
    type = schema[name];
    keys.push({
      name: name,
      type: type
    });
  }
  _results = [];
  for (key in schema) {
    value = schema[key];
    if (!TYPES[value]) {
      _results.push(attributes.push);
    } else {
      _results.push(void 0);
    }
  }
  return _results;
};

Schema.prototype.getRow = function(keys) {
  var key, queryParams, value, _results;
  queryParams = {};
  queryParams.tableName = this.tableName;
  queryParams.primaryKey = [];
  _results = [];
  for (key in keys) {
    value = keys[key];
    _results.push(queryParams.primaryKey.push({
      name: key
    }));
  }
  return _results;
};

TYPES = {
  INTEGER: 1,
  STRING: 2,
  BOOLEAN: 3,
  DOUBLE: 4,
  BINARY: 5
};

module.exports.exports = Schema;
