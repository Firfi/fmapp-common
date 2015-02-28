'use strict';

// common for xirgo and server code

var _ = require('lodash');
var moment = require('moment');
var toUnderscore = require('humps').decamelize;
var fromUnderscore = require('humps').camelize;

module.exports = function(rx) {
  return function(params) {

    var toCassandraQuery = function(log) { // {a: 'b', 'otherField': {'thirdField' : 1}} -> {a: 'b', 'other_field__third_field': 1}
      var _u = _;
      var flattenObject = function(o) {
        var res = {};
        var _f = function(o, prefix) {
          _u.map(_u.pairs(o), function(pair) {
            var k = pair[0];
            var v = pair[1];
            if (_u.isObject(v) && !_u.isArray(v)) {
              _f(v, (prefix ? prefix + '__' : '') + toUnderscore(k));
            } else {
              res[(prefix ? prefix + '__' : '') + toUnderscore(k)] = v;
            }
          });
        };
        _f(o);
        return res;
      };
      return flattenObject(log);
    };

    var cassandra = require('cassandra-driver');
    var client = new cassandra.Client({contactPoints: [params.ip], keyspace: 'demo', queryOptions: {
      prepare: true // prepare: true removes differences between JS Floats and Cassandra ints
    }}); // 128.199.51.176;
    var execute = client.execute;
    var api = _.extend(client, { // let's substitute callbacks to streams
      execute: function(query, args, opts) {
        opts = opts || {};
        // opts.prepare = true;
        if (args) {
          args = args.map(function(arg) {
            if (moment.isMoment(arg)) {
              return +arg; // to unix millis
            } else {
              return arg;
            }
          })
        }

        var stream = rx.fromNodeCallback(execute.bind(client), query, args, opts);
        stream.onError(console.error);
        return stream.map(function(result) {
          return result.rows;
        });
      },
      executeSelect: function(query, args, opts) {
        return api.execute(query, args, opts).map(function(rows) {
          // map rows back to nice js object: {row__row: 1, row_row: 2} -> {row: {row: 1}, rowRow: 2}
          return _.map(rows, function(row) {
            var res = {};
            _.map(_.pairs(row), function(pair) {
              var _k = pair[0];
              var ks = _.map(_k.split('__'), fromUnderscore);
              var lastKey = _.last(ks);
              var v = pair[1];
              var fromBigInt = function(v) {return Number(v.toString());};
              if (v && v.low) { // suddenly, it is bigInt
                v = fromBigInt(v);
              } else if (v && _.isArray(v) && !_.isEmpty(v) && _.first(v).low) { // suddenly, it is array of BigInts
                v = _.map(v, fromBigInt);
              }
              var cur = res;
              _.map(ks, function(k) {
                if (k === lastKey) {
                  cur[k] = v;
                } else {
                  cur[k] = cur[k] || {};
                  cur = cur[k];
                }
              })
            });
            return res;
          });

        });
      },
      select: function(table, where) { // AND only for now
        var tableRegex = /^[A-Za-z_]+$/;
        if (!table.match(tableRegex)) return rx.constant().map(function() {
          throw new Error('table name ' + table + ' is invalid, valid regex: ' + tableRegex);
        });
        var w = toCassandraQuery(where);
        var q = 'SELECT * FROM ' + table;
        if (!_.isEmpty(where)) {
          q += (' where ' + _.map(_.keys(w), function(col) {
            return col + ' = ?';
          }).join('AND '));
        }
        return api.executeSelect(q, _.values(where));
      },
      insert: function(o, table, cb) {
        var qo = toCassandraQuery(o);
        var query = 'INSERT INTO ' + table + ' (' + _.keys(qo).join(', ') + ') VALUES ('
          + _.map(_.values(qo), function(some) {return '?';}).join(', ') + ')';
        var args = _.values(qo);
        var res = api.execute(query, args);
        res.onValue(function(r) { // onValue call needed to really execute it
          if (cb) {
            cb(r);
          }
        });
        res.onError(function(r) {
          console.error('error in inserting in table ' + table + ' of object');
          console.error(o);
          console.error(r);
        });
        return res;
      },
      update: function(table, o, where) { // AND only for now

        var qo = toCassandraQuery(o);
        if (_.isEmpty(qo)) {
          return api.select(table, where);
        }
        var w = toCassandraQuery(where);

        var query = 'UPDATE ' + table + ' SET ' + _.map(_.keys(qo), function(k) {
          return k + ' = ?';
        }).join(', ') + ' where ' + _.map(_.keys(w), function(k) {
          return k + ' = ?';
        }).join(' AND ');
        var args = _.flatten([_.values(qo), _.values(where)], false);
        var res = api.execute(query, args);
        res.onValue(function(r) { // onValue call needed to really execute it

        });
        res.onError(function(r) {
          console.error('error in updating in table ' + table + ' of object', o, 'where', w, r);
        });
        return res;
      },
      fmappUtils: {
        toQuestionString: function(list) {
          var questionize = function(any) {return '?';};
          return _.map(list, questionize).join(', ');
        }
      }
    });
    return api;
  };
};
