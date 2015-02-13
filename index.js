'use strict';

var cassandra = require('./cassandra');

module.exports = function(rx) { // OOOOPS. https://github.com/baconjs/bacon.js/issues/522
  return {
    cassandra: cassandra(rx)
  };
};

