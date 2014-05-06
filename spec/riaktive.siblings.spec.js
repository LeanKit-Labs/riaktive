require( 'should' );
var config = require( 'configya' )( './config.json' ),
	riak = require( '../src/riaktive.js' )( config ),
	_ = require( 'lodash' );

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

	describe( 'when storing and retrieving a document with siblings', function() {
		var error,
			doc1 = { id: 'test-3', message: 'hello, world!' },
			doc2 = { id: 'test-3', message: 'hello, world!' };
		before( function( done ) {
			riak.mahBucket.put( doc1 )
				.then( function( id ) {
					done();
				} )
				.then( null, function( err ) {
					error = err;
					riak.mahBucket.put( doc2 ).done( done );
				} );
		} );

		it( 'should not error on set', function() {
			( error == undefined).should.be.true;
		} );

		it( 'should get document back', function( done ) {
			riak.mahBucket.get( 'test-3',
				function( doc ) {
					//( 1 == 2 ).should.be.true;
					done();
				},
				function( docs ) {
					docs.length.should.equal( 2 );
					done();
				},
				function( err ) {
					( err == undefined ).should.be.true();
					done();
				} );
		} );

		after( function() {
			riak.mahBucket.del( doc1 )
				.then( function() {
				} )
				.then( null, function( err ) {
					console.log( "ERR ON DELETE!!!", err );
				} );
		} );
	} );

} );