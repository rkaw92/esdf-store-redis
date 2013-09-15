// Keymaps are functions which map a sequenceID and sequenceNumber onto a redis list key and index.
function normalKeymap(sequenceID, sequenceNumber){
	return {
		key: sequenceID,
		index: sequenceNumber - 1,
		next: function nextKey(){
			return undefined;
		}
	};
}

function blocksOf100Keymap(sequenceID, sequenceNumber){
	//TODO: using a closure introduces a next() side effect - get rid of that by generating the key pointer object only based on the current one.
	var sequenceIDSuffix = sequenceID;
	var currentSequenceNumber = sequenceNumber;
	var currentBlock = Math.floor(sequenceNumber / 100);
		var next = function next(){
			var ret = {
				key: 'block' + currentBlock + ':' + sequenceIDSuffix,
				index: currentSequenceNumber,
				next: next
			};
			++currentBlock;
			currentSequenceNumber = 0;
			return ret;
		};
}

module.exports.keymaps = {
	normal: normalKeymap,
	blocksOf100: blocksOf100Keymap
};