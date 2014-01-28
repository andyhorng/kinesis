// Generated by CoffeeScript 1.6.3
(function() {
  'use strict';
  var AWS, Bacon, debug, kinesis, print, _;

  AWS = require('aws-sdk');

  Bacon = require('baconjs');

  _ = require('underscore');

  debug = require('debug');

  print = debug('creek:kinesis');

  kinesis = new AWS.Kinesis();

  exports.listen = function(StreamName) {
    return Bacon.fromNodeCallback(kinesis, 'describeStream', {
      StreamName: StreamName
    }).flatMap(function(_arg) {
      var Shards;
      Shards = _arg.StreamDescription.Shards;
      return Bacon.fromBinder(function(sink) {
        _.chain(Shards).pluck('ShardId').each(function(ShardId) {
          var options;
          options = {
            StreamName: StreamName,
            ShardId: ShardId,
            ShardIteratorType: 'LATEST'
          };
          return kinesis.getShardIterator(options, function(error, d) {
            var iterateShard;
            if (error != null) {
              return sink(new Bacon.Error(error.message));
            }
            iterateShard = function(ShardIterator) {
              return kinesis.getRecords({
                ShardIterator: ShardIterator
              }, function(error, result) {
                if (error != null) {
                  return sink(new Bacon.Error(error.message));
                }
                print('got ' + JSON.stringify(result));
                _.chain(result.Records).pluck('Data').map(function(d) {
                  return new Buffer(d, 'base64').toString();
                }).each(sink);
                if (result.Records.length > 0) {
                  return iterateShard(ShardIterator);
                } else {
                  return setTimeout(_.partial(iterateShard, ShardIterator), 200);
                }
              });
            };
            return iterateShard(d.ShardIterator);
          });
        });
        return function() {};
      });
    });
  };

  exports.publish = function(StreamName) {
    var bus;
    bus = new Bacon.Bus();
    bus.filter(_.isObject).map(function(d) {
      var encoded;
      encoded = (new Buffer(JSON.stringify(d))).toString('base64');
      return {
        StreamName: StreamName,
        Data: encoded,
        PartitionKey: d.log_id || _.uniqueId()
      };
    }).onValue(function(args) {
      print('publish ' + JSON.stringify(args));
      return Bacon.fromNodeCallback(kinesis, 'putRecord', args).onError(function(err) {
        return print('got error: ' + err);
      });
    });
    return bus;
  };

}).call(this);
