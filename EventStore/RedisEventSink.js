var when = require('when');

function RedisEventSink(client){
	this._client = client;
	//TODO: this._client.on('error') ?
}

// This is the Lua script used when saving a commit. It checks the sequence length against the expected number, and if it matches, appends the commit (otherwise, returns false).
//  Since Lua scripts are executed atomically within redis, it pushes a dispatch pointer (to maintain global commit ordering) after the commit is saved.
//  On errors, an error is returned to the EVAL() callback.
var transactionalAggregateAppend = [
	"local list_length = redis.call('LLEN', KEYS[1])",
	"if (list_length == tonumber(ARGV[1])) then",
	"	redis.call('RPUSH', KEYS[1], ARGV[2])",
	"	redis.call('RPUSH', KEYS[2], ARGV[3])",
	"	return true",
	"else",
	"	return false",
	"end"
].join("\n");

var sequencePrefix = 'sequence:';
var dispatchPrefix = 'dispatch:';
var dispatchGlobalKey = 'global';

RedisEventSink.prototype.sink = function sink(commit){
	var self = this;
	var sinkFuture = when.defer();
	var commitPayload = JSON.stringify(commit);
	// The commit is appended as a pointer only to save space. A streamer is then supposed to take those pointers, read the respective commits and publish them.
	var commitDispatchEnvelope = JSON.stringify({'sequenceID': commit.sequenceID, 'sequenceSlot': commit.sequenceSlot});
	var dispatchKey = dispatchPrefix + dispatchGlobalKey;
	var sequenceKey = sequencePrefix + commit.sequenceID;
	// Compute the expected length - if it differs, redis will signal a concurrency exception (by returning false).
	var expectedSequenceLength = commit.sequenceSlot - 1;
	// Note: capital EVAL is used to shut jshint up [ https://github.com/jshint/jshint/issues/1204 ].
	self._client.EVAL([transactionalAggregateAppend, 2, sequenceKey, dispatchKey, expectedSequenceLength, commitPayload, commitDispatchEnvelope], function(err, result){
		// Note: Lua boolean true is mapped to redis numeric value 1, and false is mapped to nil (null in JS)
		if(!err && result == 1){
			// No error reported and the Lua script returned true, so both the dispatch message and the commit must have been saved.
			sinkFuture.resolver.resolve(true);
		}
		else{
			// Either there was an error or a concurrency exception. Since they both require a reload and retry, signal them all the same.
			sinkFuture.resolver.reject(err ? err : new Error('Concurrency exception'));
		}
	});
	return sinkFuture.promise;
};

RedisEventSink.prototype.rehydrate = function rehydrate(object, sequenceID, since){
	var rehydrateFuture = when.defer();
	var sinceIndex = since ? (Number(since) - 1) : 0; // Commit #1 is element #0. Starts from index 0 by default.
	var untilIndex = -1; // The last element (-1 = the first from the end).
	var loadSequenceKey = sequencePrefix + sequenceID;
	this._client.lrange(loadSequenceKey, sinceIndex, untilIndex, function(err, serializedCommits){
		if(err){
			rehydrateFuture.resolver.reject(err);
			return;
		}
		for(var i = 0; i < serializedCommits.length; ++i){
			var thisCommit = JSON.parse(serializedCommits[i]);
			object.applyCommit(thisCommit);
			rehydrateFuture.resolver.notify(i);
		}
		rehydrateFuture.resolver.resolve(object);
	});
	return rehydrateFuture.promise;
};

module.exports.RedisEventSink = RedisEventSink;