'use strict'

AWS     = require 'aws-sdk'
Bacon   = require 'baconjs'
_       = require 'underscore'
debug   = require 'debug'

print = debug 'creek:kinesis'


exports.listen = (StreamName, opts={}) ->
  _.defaults opts, {
    region: 'us-east-1'
  }
  kinesis = new AWS.Kinesis(opts)

  Bacon
    .fromNodeCallback(kinesis, 'describeStream', {StreamName})
    .flatMap ({StreamDescription: {Shards}}) ->
      Bacon.fromBinder (sink) ->
        _.chain(Shards)
        .pluck('ShardId')
        .each (ShardId) ->
          options = {
            StreamName
            ShardId
            ShardIteratorType: 'LATEST'
          }
          kinesis.getShardIterator options, (error, d) ->
            return sink new Bacon.Error error.message if error?

            iterateShard = (ShardIterator) ->
              kinesis.getRecords {ShardIterator}, (error, result) ->
                return sink new Bacon.Error error.message if error?

                print 'got ' + JSON.stringify result

                _.chain(result.Records)
                .pluck('Data')
                .map (d) ->
                  buf = new Buffer(d, 'base64').toString()
                  try
                    return JSON.parse buf
                  catch e
                    print 'bad json'
                    return new Bacon.Error(e.message)
                .each(sink)

                if result.Records.length > 0
                  iterateShard ShardIterator
                else
                  # throttle
                  setTimeout _.partial(iterateShard, ShardIterator), 200

            iterateShard d.ShardIterator
        return ->
          # do-op

exports.publish = (StreamName, opts={}) ->
  _.defaults opts, {
    region: 'us-east-1'
  }
  kinesis = new AWS.Kinesis(opts)

  bus = new Bacon.Bus()
  bus
    .filter(_.isObject)
    .map (d) ->
      encoded = (new Buffer(JSON.stringify d)).toString('base64')
      {StreamName, Data: encoded, PartitionKey: d.log_id or _.uniqueId()}
    .onValue (args) ->
      print 'publish ' + JSON.stringify args
      Bacon
        .fromNodeCallback(kinesis, 'putRecord', args)
        .onError (err) ->
          print 'got error: ' + err
  bus
