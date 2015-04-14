require( '../setup' );

function noOp() {
}

describe( 'when creating a new index manager', function() {
	var riak, IndexMgr, indexMgr;
	before( function() {
		riak = noOp;
		IndexMgr = require( '../../src/indexManager.js' );
		indexMgr = new IndexMgr( riak );
	} );
	it( 'has an empty set of indexes', function() {
		indexMgr.indexes.should.eql( {} );
	} );

	it( 'it has a create function', function() {
		_.isFunction( indexMgr.create ).should.be.true;
	} );
} );
