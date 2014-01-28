'use strict'

{assert} = require 'chai'
kinesis  = require "#{__dirname}/../src/kinesis"

describe 'kinesis bacon stream', ->
  it 'should pub/sub', (done) ->
    @timeout 8 * 1000
    input  = kinesis.listen 'test'
    output = kinesis.publish 'test'

    input.onValue (data) ->
      console.log JSON.stringify data
      assert.propertyVal data, 'foo', 'bar'
      do done

    output.push {foo: 'bar'}
