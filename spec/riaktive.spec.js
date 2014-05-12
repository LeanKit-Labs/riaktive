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
		riak.createBucket( 'mahBucket', { allow_mult: true } )
			.done( function( b ) {
				bucket = b;
				done();
			} );	
	} );

	it( 'should return a bucket', function () {
		bucket.should.be.ok;
	} );

	it( 'should create bucket', function() {
		riak[ 'mahBucket' ].should.be.ok;
	} );
	
	describe( 'when requesting non-existent key', function() {
		var doc = {};
		before( function( done ) {
			riak.mahBucket.get( 'lolololol'
				, function( result ) {
					doc = result;
					done();
				}, function( docs ) {
					done();
				}, function( err ) {
					done();
				} )
		} );

		it( 'should return undefined', function() {
			( doc === undefined ).should.be.true;
		} );
	} )

	describe( 'when storing and retrieving a document', function() {
		var error;
		before( function( done ) {
			riak.mahBucket.put( 'test-1', { message: 'hello, world!' } )
				.then( function() { done(); } )
				.then( null, function( err ) {
					error = err;
					done();
				} );
		} );

		it( 'should not error on set', function() {
			( error == undefined).should.be.true;
		} );

		it( 'should get document back', function( done ) {
			riak.mahBucket.get( 'test-1',
				function( doc ) {
					_.omit( doc, 'vclock' ).should.eql( { id: 'test-1', message: 'hello, world!' } );
					done();
				},
			 	function( docs ) {

			 	},
				function( err ) {
					( err == undefined ).should.be.true();
					done();
				} );
		} );

		after( function() {
			riak.mahBucket.del( 'test-1' )
				.then( function() {
				} )
				.then( null, function( err ) {
					console.log( "ERR ON DELETE!!!", err );
				} );
			riak.mahBucket.del( '' );
		} );
	} );

	describe( 'when storing and retrieving a document with id property', function() {
		var error,
			doc = { id: 'test-2', message: 'hello, world!' };
		before( function( done ) {
			riak.mahBucket.put( doc )
				.then( function( id ) { 
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
			riak.mahBucket.get( 'test-2',
				function( doc ) {
					_.omit( doc, 'vclock' ).should.eql( { id: 'test-2', message: 'hello, world!' } );
					done();
				},
				function( docs ) {
					_.omit( docs[ 0 ], 'vclock' ).should.eql( { id: 'test-2', message: 'hello, world!' } );
					done();
				},
				function( err ) {
					( err == undefined ).should.be.true();
					done();
				} );
		} );

		after( function() {
			riak.mahBucket.del( doc )
				.then( function() {
				} )
				.then( null, function( err ) {
					console.log( "ERR ON DELETE!!!", err );
				} );
		} );
	} );

	describe( 'when updating a document without a vclock', function () {
		var original = { id: 'update-1', message: 'this is version 1' },
			next = { id: 'update-1', message: 'this is version 2' },
			result;

		before( function( done ) {
			riak.mahBucket.put( original )
				.done( function() {
					riak.mahBucket.put( next )
						.done( function() { done(); } );
				} );
		} );

		it( 'should cause siblings', function( done ) {
			riak.mahBucket.get( 'update-1' )
				.then( function( doc ) {
					result = doc;
					_.isArray( result ).should.be.true;
					result[ 1 ].should.eql( next );
					done();		
				} );
		} );

		after( function() {
			riak.mahBucket.del( next );
		} );
	} );

	describe( 'when updating a document with a vclock', function () {
		var original = { id: 'update-2', message: 'this is version 1' },
			result;

		before( function( done ) {
			riak.mahBucket.put( original )
				.done( function() {
					original.message = 'this is version 2'
					riak.mahBucket.put( original )
						.done( function() { done(); } );
				} );
		} );

		it( 'should not cause siblings', function( done ) {
			riak.mahBucket.get( 'update-2' )
				.then( function( doc ) {
					result = doc;
					_.isArray( result ).should.be.false;
					result.should.eql( original );
					done();
				} );
		} );

		after( function() {
			riak.mahBucket.del( original );
		} );
	} );

	describe( 'when applying a change to a document', function () {
		var original = { id: 'apply-1', message: 'this is version 1', version: 1 },
			expected = { id: 'apply-1', message: 'this is version 2', version: 2 },
			mutation = function( doc ) {
				doc.version ++;
				doc.message = 'this is version ' + doc.version;
				return doc;
			},
			result;

		before( function( done ) {
			riak.mahBucket.put( original, { 'test-index': 100 } )
				.then( function() {
					riak.mahBucket.mutate( 'apply-1', mutation )
						.then( null, function( err ) {
							console.log( err );
							done();
						} )
						.then( function( changed ) {
							result = changed;
							done();
						} );
				} );
		} );

		it( 'should return expected result', function( done ) {
			riak.mahBucket.get( 'apply-1' )
				.then( function( doc ) {
					_.isArray( result ).should.be.false;
					result.should.eql( doc );
					console.log( doc );
					done();
				} );
		} );

		after( function() {
			riak.mahBucket.del( original );
		} );
	} );

} );