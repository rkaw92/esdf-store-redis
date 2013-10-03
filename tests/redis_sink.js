var redis = require('redis');
var uuid = require('uuid');
var assert = require('assert');
var client = redis.createClient();
var RedisEventSink = require('../EventStore/RedisEventSink.js').RedisEventSink;
var ESDF = require('esdf');
var Event = ESDF.core.Event;
var Commit = ESDF.core.Commit;
var EventSourcedAggregate = ESDF.core.EventSourcedAggregate;
var tryWith = ESDF.utils.tryWith;

function DummyAR(){
	this._ready = false;
}
DummyAR.prototype = new EventSourcedAggregate();
DummyAR.prototype.onDummyAREvent = function(event, commitMetadata){
	this._ready = true;
};

describe('RedisEventSink', function(){
	describe('.sink', function(){
		it('should save a commit successfully into a Redis list', function(done){
			var sink = new RedisEventSink(client);
			sink.sink(new Commit([new Event('TestEventType1', {key1: 'val1', key2: 1337})], 'testseq' + uuid.v4(), 1)).then(function(result){
				done();
			}, function(reason){
				done(reason ? reason : new Error('Redis sink test: Unknown failure reason'));
			});
		});
		it('should save 100 commits into Redis without taking forever to do it', function(done){
			var requiredSaves = 100;
			var sink = new RedisEventSink(client);
			var sunk = 0;
			function finishSingle(){
				++sunk;
				if(sunk === requiredSaves){
					done();
				}
			}
			var lotsUUID = uuid.v4();
			for(var i = 1; i <= requiredSaves; ++i){
				sink.sink(new Commit([new Event('BulkCrapEvent', {fat: 'somewhat'})], 'LotsOfMe' + lotsUUID, i)).then(finishSingle, done);
			}
		});
		var dummyID = 'dummy-' + uuid.v4();
		it('should manage to rehydrate an object after saving a commit', function(done){
			var dummyObject = new DummyAR();
			var sink = new RedisEventSink(client);
			dummyObject._aggregateID = dummyID;
			dummyObject._eventSink = sink;
			dummyObject._stageEvent(new Event('DummyAREvent', {useless: true}));
			dummyObject.commit().then(function(){
				var secondInstance = new DummyAR();
				sink.rehydrate(secondInstance, dummyID).then(function(){
					assert(secondInstance._ready);
					done();
				});
			});
		});
		it('should be suitable as a loader backend for tryWith', function(done){
			var sink = new RedisEventSink(client);
			var loader = ESDF.utils.createAggregateLoader(sink);
			tryWith(loader, DummyAR, dummyID, function(AR){
				assert.strictEqual(AR._ready, true);
			}).then(done);
		});
	});
});