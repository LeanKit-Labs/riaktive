require( '../setup' );
var IndexMgr = require( '../../src/indexManager.js' );

function noOp() {
}

describe( 'indexManager', function() {

	describe( 'when creating a new index manager', function() {
		var indexMgr;
		before( function() {
			indexMgr = new IndexMgr( noOp );
		} );
		it( 'has an empty set of indexes', function() {
			indexMgr.indexes.should.eql( {} );
		} );

		it( 'it has a create function', function() {
			_.isFunction( indexMgr.create ).should.be.true;
		} );
	} );

	describe( 'when creating an index', function() {
		var indexMgr;

		describe( 'and the index already exists', function() {
			var riak;
			before( function() {
				riak = {
					yzGetIndex: noOp
				};
				sinon.stub( riak, 'yzGetIndex', function() {
					var fake = {
						index: [
							{ schema: {} }
						]
					};
					return when( fake );
				} );
				indexMgr = new IndexMgr( riak );
				debugger;
				return indexMgr.create( 'index1', 'foo' )
					.then( function() {
						return indexMgr.create( 'index1', 'foo' );
					} );
			} );
			it( 'returns the existing index', function() {} );
		} );
	} );
} );
