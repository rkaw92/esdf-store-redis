var redis = require('redis');
var uuid = require('uuid');
var when = require('when');
var RedisEventSink = require('../EventStore/RedisEventSink.js').RedisEventSink;
var RedisEventStreamer = require('../EventStore/RedisEventStreamer.js').RedisEventStreamer;
var ESDF = require('esdf');
var Event = ESDF.core.Event;
var Commit = ESDF.core.Commit;
var EventSourcedAggregate = ESDF.core.EventSourcedAggregate;
var repl = require('repl');

var sinkClient = redis.createClient();
var streamClient = redis.createClient();
var messagingClient = redis.createClient();
var sink = new RedisEventSink(sinkClient);
var streamer = new RedisEventStreamer(streamClient, messagingClient, 'test-streamer-basic', {persistent: true, pollingDelay: 50});
var testID = 'manualtest-simplestream-' + uuid.v4();

streamer.setPublisher({
	publishCommit: function(commit){
		//console.log('& Got a commit. First event type:', commit.events[0].eventType);
		return when.resolve();
	}
});
streamer.start();
var sequenceSlot = 1;
setInterval(function(){
	sink.sink(new Commit([new Event('StreamTestEv', {test: true})], testID, sequenceSlot++)).then(function _commitSunk(){
	});
}, 1000);

repl.start({prompt: 'simplestream> '}).context.streamer = streamer;