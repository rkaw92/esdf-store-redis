var redis = require('redis');
var uuid = require('uuid');
var when = require('when');
var RedisEventSink = require('../EventStore/RedisEventSink.js').RedisEventSink;
var RedisEventStreamer = require('../EventStore/RedisEventStreamer.js').RedisEventStreamer;
var ESDF = require('esdf');
var Event = ESDF.core.Event;
var Commit = ESDF.core.Commit;
var EventSourcedAggregate = ESDF.core.EventSourcedAggregate;

//NOTE: please issue a FLUSHDB before running this test.

describe('RedisEventStreamer', function(){
	describe('.start', function(){
		it('should dispatch a message to the publisher function without using messaging', function(done){
			var sinkClient = redis.createClient();
			var streamClient = redis.createClient();
			var sink = new RedisEventSink(sinkClient);
			var streamer = new RedisEventStreamer(streamClient, undefined, 'test-streamer-basic', {persistent: true, pollingDelay: 200});
			sink.sink(new Commit([new Event('StreamTestEv', {test: true})], 'streamtest-' + uuid.v4(), 1)).then(function _commitSunk(){
				streamer.setPublisher({
					publishCommit: function(commit){done(); streamer.pause(); streamClient.end(); return when.resolve();}
				});
				streamer.start();
			});
		});
		it('should dispatch a message to the publisher function with the use of redis pub-sub messaging', function(done){
			var sinkClient = redis.createClient();
			var streamClient = redis.createClient();
			var messagingClient = redis.createClient();
			var sink = new RedisEventSink(sinkClient);
			var streamer = new RedisEventStreamer(streamClient, messagingClient, 'test-streamer-basic', {persistent: true, pollingDelay: 50});
			sink.sink(new Commit([new Event('StreamTestEv', {test: true})], 'streamtest-' + uuid.v4(), 1)).then(function _commitSunk(){
				streamer.setPublisher({
					publishCommit: function(commit){done(); return when.resolve();}
				});
				streamer.start();
			});
		});
	});
});