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
		/*
		it('should dispatch a message to the publisher function without using messaging', function(done){
			var sinkClient = redis.createClient();
			var streamClient = redis.createClient();
			var sink = new RedisEventSink(sinkClient);
			var streamer = new RedisEventStreamer(streamClient, undefined, 'test-streamer-basic', {persistent: true, pollingDelay: 100});
			var streamID = 'streamtest-' + uuid.v4();
			sink.sink(new Commit([new Event('StreamTestEv', {test: true})], streamID, 1)).then(function _commitSunk(){
				streamer.setPublisher({
					publishCommit: function(commit){
						if(commit.sequenceID === streamID){
							done();
							streamer.pause();
							streamClient.end();
						}
						return when.resolve();
					}
				});
				streamer.start();
			});
		});
	*/
		it('should dispatch a message to the publisher function with the use of redis pub-sub messaging', function(done){
			var sinkClient = redis.createClient();
			var streamClient = redis.createClient();
			var messagingClient = redis.createClient();
			var sink = new RedisEventSink(sinkClient);
			var streamer = new RedisEventStreamer(streamClient, messagingClient, 'test-streamer-basic', {persistent: true, pollingDelay: 50});
			var testID = 'streamtest-' + uuid.v4();
			
			var publishedMeAlready = false;
			function almostDone(){
				if(!publishedMeAlready){
					publishedMeAlready = true;
					setTimeout(function(){
						streamer._markNextIndex().then(done.bind(undefined, undefined));
					}, 10);
				}
			};
			streamer.setPublisher({
				publishCommit: function(commit){
					if(commit.sequenceID === testID){
						almostDone();
					}
					return when.resolve();
				}
			});
			streamer.start();
			setTimeout(function(){
				sink.sink(new Commit([new Event('StreamTestEv', {test: true})], testID, 1)).then(function _commitSunk(){
					console.log('* Test commit sunk');
				});
			}, 10);
		});
	});
	describe('.disengage', function(){
		it('should safely stop a running streaming process and then resume it');
	});
});