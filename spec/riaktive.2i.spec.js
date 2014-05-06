require( 'should' );
var config = require( 'configya' )( './config.json' ),
	riak = require( '../src/riaktive.js' )( config ),
	_ = require( 'lodash' ),
	when = require( 'when' );

describe( 'when connecting to riak', function() {
	before( function( done ) {
		riak.connect()
			.done( function() { done(); } );
	} );

	it( 'should be connected', function() {
		riak.connected.should.be.true;
	} );

} );

describe( 'when creating a plain bucket', function() {
	
	var bucket;
	before( function( done ) {
		this.timeout( 20000 );
		riak.createBucket( 'mahBucket', {} )
			.done( function( b ) {
				bucket = b;
				done();
			} );	
	} );

	describe( 'when storing and retrieving documents with matching secondary indices', function() {
		var error,
			doc1 = { id: 'test-4', message: 'hello, world!' },
			doc2 = { id: 'test-5', message: 'hello, world!' },
			keys = [];
		before( function( done ) {
			riak.mahBucket.put( doc1, { 'lookup': 1 } )
				.then( function( id ) {
					done();
				} )
				.then( null, function( err ) {
					error = err;
					console.log( 'err on put', err );
					done();
				} );

			riak.mahBucket.put( doc2, { 'lookup': 1 } );
		} );

		it( 'should not error on set', function() {
			( error == undefined).should.be.true;
		} );

		it( 'should get document back', function( done ) {
			riak.mahBucket.getKeysByIndex( 'lookup', 1 )
				.progress( function( key ) {
					keys = keys.concat( key.keys );
				} )
				.then( null, function( urr ) {
					console.log( 'error getting doc', urr );
					done();
				} )
				.done( function() {
					keys.sort();
					keys.should.eql( [ 'test-4', 'test-5' ] );
					done();
				} );
		} );

		it( 'should retrieve multiple documents', function( done ) {
			var docs = [];
			riak.mahBucket.getByKeys( keys )
				.progress( function( doc ) {
					docs.push( doc );
				} )
				.done( function( list ) {
					list = _.uniq( list, function( x ) {
						return x.id;
					} );
					list.length.should.equal( 2 );
					docs.length.should.equal( list.length );
					list.should.eql( docs );
					_.map( list, function( l ) { return _.omit( l, '_indices' ); } )
						.should.eql( [ doc1, doc2 ] );
					done();
				} );
		} );

		after( function() {
			riak.mahBucket.del( 'test-4' )
				.then( function() {
				} )
				.then( null, function( err ) {
					console.log( "ERR ON DELETE!!!", err );
				} );
			riak.mahBucket.del( 'test-5' );
		} );
	} );

	describe( 'when storing and retrieving document pages with matching secondary indices', function() {
		var error,
			doc1 = { id: 'test-6', message: 'hello, world!' },
			doc2 = { id: 'test-7', message: 'hello, world!' },
			doc3 = { id: 'test-8', message: 'hello, world!' },
			doc4 = { id: 'test-9', message: 'hello, world!' },
			keys = [];
		before( function( done ) {
			when.all( [
				riak.mahBucket.put( doc1, { 'lookup': 1 } ),
				riak.mahBucket.put( doc2, { 'lookup': 1 } ),
				riak.mahBucket.put( doc3, { 'lookup': 1 } ),
				riak.mahBucket.put( doc4, { 'lookup': 1 } )
			] )
			.done( function() { done(); } );
		} );

		it( 'should not error on set', function() {
			( error == undefined).should.be.true;
		} );

		it( 'should get keys back', function( done ) {
			var onKeys = function( key ) {
					keys = keys.concat( key.keys );
				};
			riak.mahBucket.getKeysByIndex( '$key', 'test-6', 'test7', 2 )
				// 	{ index: '$key', start: '!', finish: '~', limit: 2 } 
				// )
				.progress( onKeys )
				.then( null, function( urr ) {
					console.log( 'error getting doc', urr );
					done();
				} )
				.done( function( continuation ) {
					keys.sort();
					keys.should.eql( [ 'test-6', 'test-7' ] );
					riak.mahBucket.getKeysByIndex( continuation )
						.progress( onKeys )
						.done( function() {
							keys.sort();
							keys.should.eql( [ 'test-6', 'test-7', 'test-8', 'test-9' ] );
							done();
						} );
				} );
		} );

		after( function() {
			riak.mahBucket.del( 'test-6' )
				.then( function() {
				} )
				.then( null, function( err ) {
					console.log( "ERR ON DELETE!!!", err );
				} );
			riak.mahBucket.del( 'test-7' );
			riak.mahBucket.del( 'test-8' );
			riak.mahBucket.del( 'test-9' );
		} );
	} );
} );

