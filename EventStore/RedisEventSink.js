var when = require('when');
var nodefn = require('when/node');
var esdf = require('esdf');

/**
 * Construct a new RedisEventSink instance.
 * A Redis-based event sink saves aggregate commits in per-AR lists, while atomically pushing their IDs onto a separate, global dispatch list.
 * The streamer then reads this global list incrementally and dispatches messages by getting them from the individual lists.
 * Requires Redis 2.6 (with Lua script EVAL support). Will not run on Redis 2.4.
 * @param {external:RedisClient} client A Redis client object, as returned by the "redis" module's createClient().
 * @constructor
 */
function RedisEventSink(client){
	this._client = client;
	//TODO: this._client.on('error') ?
	this._readSize = 100;
}

/** This is the Lua script used when saving a commit. It checks the sequence length against the expected number, and if it matches, appends the commit (otherwise, returns false).
 *  Since Lua scripts are executed atomically within redis, it pushes a dispatch pointer (to maintain global commit ordering) after the commit is saved.
 *  On infrastructural (non-logic) errors, an error is returned to the EVAL() callback.
 */
var transactionalAggregateAppend = [
	"local list_length = redis.call('LLEN', KEYS[1])",
	"if (list_length == tonumber(ARGV[1])) then",
	"	redis.call('RPUSH', KEYS[1], ARGV[2])",
	"	redis.call('RPUSH', KEYS[2], ARGV[3])",
	"	return redis.call('LLEN', KEYS[2])", // get the length of the dispatch list after the push
	"else",
	"	return 0",
	"end"
].join("\n");

var sequencePrefix = 'sequence:';
var dispatchPrefix = 'dispatch:';
var dispatchGlobalKey = 'global';
var dispatchNotificationChannel = 'dispatch-notify';

/**
 * Save a commit to the database.
 * @param {module:esdf/core/Commit} commit The commit object to save. Saved under the sequence ID and slot indicated in the commit's properties.
 * @returns {external:Promise} A promise that resolves when the saving process is complete.
 */
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
	self._client.EVAL([transactionalAggregateAppend, 2, sequenceKey, dispatchKey, expectedSequenceLength, commitPayload, commitDispatchEnvelope], function(evalError, listNewLength){
		// Note: Lua boolean true is mapped to redis numeric value 1, and false is mapped to nil (null in JS).
		if(!evalError && listNewLength !== 0){
			// No error reported and the Lua script returned the new dispatch list length, so both the dispatch message and the commit must have been saved.
			self._client.publish(dispatchNotificationChannel, JSON.stringify({sequenceID: commit.sequenceID, sequenceSlot: commit.sequenceSlot, dispatchIndex: listNewLength - 1}), function(err, result){
				// Result deliberately ignored. There is no harm in failed notifications, as long as the streamer is aware of the possibility and does periodic polling.
			});
			sinkFuture.resolver.resolve(true);
		}
		else{
			// Either there was an error or a concurrency exception. Since they both require a reload and retry, signal them all the same.
			if(evalError){
				sinkFuture.resolver.reject(evalError);
			}
			else{
				var concurrencyException = new Error('Concurrency exception');
				concurrencyException.labels = {
					isRetriable: true
				};
				sinkFuture.resolver.reject(concurrencyException);
			}
		}
	});
	return sinkFuture.promise;
};

/**
 * Get a ReadableStream of commits comprising a given event sequence.
 * @param {string} sequenceID - ID of the sequence. Typically equal to the Aggregate Root ID.
 * @param {number} since - The sequence slot to start reading at. Note that these are counted from zero.
 */
RedisEventSink.prototype.getCommitStream = function getCommitStream(sequenceID, since) {
	var self = this;
	var keyToLoad = sequencePrefix + sequenceID;
	
	return new esdf.utils.CommitStream(function(sequenceID, currentOffset) {
		// Call LRANGE sequence:<sequenceID> <currentOffset> <currentOffset + readSize>
		//TODO: If we know from a previous call that the stream has been exhausted, skip the last LRANGE and just return null immediately. This saves us one round-trip on each load.
		return nodefn.call(self._client.lrange.bind(self._client), keyToLoad, currentOffset - 1, currentOffset - 1 + self._readSize).then(function(commitStrings) {
			// Tell the upper layer when the reading has ended:
			return commitStrings.length > 0 ? commitStrings.map(function(commitString) {
				return JSON.parse(commitString);
			}) : null;
		}).catch(function(error) {
			console.error('LRANGE >>> ERROR %s', error);
		});
	}, sequenceID, since);
};

/**
 * Re-apply commits under a particular sequence (aggregate) ID to an object. Commits are applied synchronously, one after another.
 * Requires that the object present an applyCommit function.
 * @param {module:esdf/core/EventSourcedAggregate} object The object which should accept the commits.
 * @param {string} sequenceID The stream ID to read when fetching the commits.
 * @param {number} since Which sequence slot to begin with. 1 is the first commit in the stream.
 * @see {module:esdf/core/EventSourcedAggregate.applyCommit}
 */
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
		try{
			for(var i = 0; i < serializedCommits.length; ++i){
				var thisCommit = JSON.parse(serializedCommits[i]);
				object.applyCommit(thisCommit);
				rehydrateFuture.resolver.notify(i);
			}
			rehydrateFuture.resolver.resolve(object);
		}
		catch(rehydrationError){
			rehydrateFuture.resolver.reject(rehydrationError);
			return;
		}
	});
	return rehydrateFuture.promise;
};

module.exports.RedisEventSink = RedisEventSink;