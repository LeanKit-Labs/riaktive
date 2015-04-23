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

		describe( 'and it is new', function() {
			var riak, timeoutVal;
			before( function() {
				riak = {
					yzGetIndex: noOp,
					yzPutIndex: noOp
				};
				sinon.stub( riak, 'yzGetIndex', function() {
					return when( {} );
				} );
				sinon.stub( riak, 'yzPutIndex', function() {
					return when();
				} );
				indexMgr = new IndexMgr( riak );

				return indexMgr.create( 'index1', 'foo' )
					.then( function( tm ) {
						timeoutVal = tm;
					} );
			} );

			it( 'checks for an existing index', function() {
				riak.yzGetIndex.withArgs( { name: 'index1' } ).calledOnce.should.be.true;
			} );

			it( 'creates a new index', function() {
				riak.yzPutIndex.withArgs( { index: { name: 'index1', schema: 'foo' } } )
					.calledOnce.should.be.true;
			} );
			it( 'returns a timeout value of 10 seconds', function() {
				timeoutVal.should.equal( 10000 );
			} );
		} );

		describe( 'and the index already exists', function() {
			var riak, timeoutVal;
			before( function() {
				riak = {
					yzGetIndex: noOp,
					yzPutIndex: noOp
				};
				sinon.stub( riak, 'yzGetIndex', function() {
					var fake = {
						index: [
							{ schema: {} }
						]
					};
					return when( fake );
				} );
				sinon.stub( riak, 'yzPutIndex', function() {
					return when();
				} );

				indexMgr = new IndexMgr( riak );

				return indexMgr.create( 'index1', 'foo' )
					.then( function( val ) {
						return indexMgr.create( 'index1', 'foo' )
							.then( function( tm ) {
								timeoutVal = tm;
							} );
					} );
			} );
			it( 'checks for an existing index', function() {
				riak.yzGetIndex.calledOnce.should.be.true;
			} );

			it( 'does not create a new index', function() {
				riak.yzPutIndex.calledOnce.should.be.true;
			} );
			it( 'returns a timeout value of 0 seconds', function() {
				timeoutVal.should.equal( 0 );
			} );
		} );
	} );
} );
