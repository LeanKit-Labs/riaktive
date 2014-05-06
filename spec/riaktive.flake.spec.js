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

	describe( 'when storing and retrieving a document with flake ids', function() {
		var error,
			key;
		before( function( done ) {
			riak.mahBucket.put( { message: 'hello, world!' } )
				.then( function( id ) { 
					key = id;
					done(); 
				} )
				.then( null, function( err ) {
					error = err;
					done();
				} );
		} );

		it( 'should not error on set', function() {
			( error == undefined).should.be.true;
		} );

		it( 'should get document back', function( done ) {
			riak.mahBucket.get( key,
				function( doc ) {
					_.omit( doc, 'vclock' ).should.eql( { id: key, message: 'hello, world!' } );
					done();
				},
				function( err ) {
					( err == undefined ).should.be.true();
					done();
				} );
		} );

		after( function() {
			riak.mahBucket.del( key )
				.then( function() {
				} )
				.then( null, function( err ) {
					console.log( "ERR ON DELETE!!!", err );
				} );
		} );
	} );
} );