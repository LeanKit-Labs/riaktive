var _ = require( 'lodash' );
var should = require( 'should' ); // jshint ignore:line
var seq = require( 'when/sequence' );
var connect = require( '../src/connection.js' );
var config = require( 'configya' )( './config.json', { riak: { server: 'ubuntu' } } );

describe( 'when creating a bucket', function() {
	var riak, props, fetched, mutated, siblings, resolved, tmp,
		keys = [],
		list1 = [],
		list2 = [];

	before( function( done ) {
		this.timeout( 20000 );
		riak = connect( { host: config.riak.server } );
		var bucket = riak.bucket( [ 'mah', 'bucket' ], { alias: 'mahBucket' } );
		// here is a descriptions of the steps this sequence takes
		// 1. read the bucket properties
		// 2. create a document at 'test-key-1' with secondary index of 'lookup_int': 10
		// 3. retrieve the document created in the previous step
		// 4. mutate the original document by adding a field (this should not create a sibling)
		// 5. get the mutated document
		// 6. create a second document at 'test-key-2' with secondary index of 'lookup_int': 11
		// 7. retrieve keys of the documents by range 1 - 20 for 'lookup_int'
		// 8. get documents for the keys retrieved by index in step 7
		// 9. get documents via index range in a single step
		// 10. create a new document at 'test-key-3'
		// 11. put a new document version to 'test-key-3'
		// 12. get 'test-key-3' to verify creation of siblings
		// 13. resolve divergence with the latter sibling
		// 14. get 'test-key-3' to verify single document
		// 15 - 17. delete the keys created as part of this sequence
		seq( [
				function() { return riak.getBucket( { bucket: 'mah_bucket' } ); },
				function() { return riak.mahBucket.put( 'test-key-1', { message: 'hulloo', aList: [ 'a', 'b', 'c' ] }, { lookup: 10 } ); },
				function() { return riak.mahBucket.get( 'test-key-1' ); },
				function() { return bucket.mutate( 'test-key-1', function( doc ) {
					doc.subject = 'greeting';
					doc._indexes.lookup = [ 10, 40 ];
					return doc;
				} ); },
				function() { return bucket.get( 'test-key-1' ); },
				function() { return bucket.put( 'test-key-2', { message: 'hulloo to you too' }, { lookup: 11 } ); },
				function() { 
					return bucket.getKeysByIndex( 'lookup', 1, 20 )
						.progress( function( data ) {
							keys = data.concat( keys );
						} );
				},
				function() {
					return bucket.getByKeys( keys )
						.progress( function( record ) {
							list1.push( record );
						} );
				},
				function() {
					return bucket.getByIndex( 'lookup', 1, 20 )
						.progress( function( record ) {
							list2.push( record );
						} );
				},
				function() {
					return bucket.put( 'test-key-3', { answer: 'nope' } );
				},
				function() {
					return bucket.put( 'test-key-3', { answer: 'yarp' } );
				},
				function() {
					return bucket.get( 'test-key-3' )
						.then( function( doc ) {
							tmp = doc;
							return doc;
						} );
				},
				function() { return bucket.put( 'test-key-3', tmp[ 1 ] ); },
				function() { return bucket.get( 'test-key-3' ); },
				function() { return bucket.del( 'test-key-1' ); },
				function() { return bucket.del( 'test-key-2' ); },
				function() { return bucket.del( 'test-key-3' ); }
			] )
		.then( function( results ) {
			props = results[ 0 ].props;
			fetched = results[ 2 ];
			mutated = results[ 4 ];
			siblings = results[ 11 ];
			resolved = results[ 13 ];
			done();
		} )
		.then( null, function( err ) {
			console.log( 'failed with', err, err.stack );
			done();
		} )
		.catch( function( err ) {
			console.log( 'errored with', err, err.stack );
			done();
		} );
	} );

	it( 'should create valid bucket properties', function() {
		props.search_index.should.equal( 'mah_bucket_index' );
		props.allow_mult.should.be.true; //jshint ignore:line
	} );

	it( 'should resolve operations after setup', function() {
		fetched.message.should.equal( 'hulloo' );
	} );

	it( 'should correctly parse indices from doc', function() {
		fetched._indexes.lookup.should.equal( 10 );
	} );

	it( 'should mutate without creating siblings', function() {
		_.omit( mutated, '_indexes', 'vclock' ).should.eql( {
			id: 'test-key-1',
			subject: 'greeting',
			message: 'hulloo',
			aList: [ 'a', 'b', 'c' ]
		} );
	} );

	it( 'should correctly parse mutated indexes', function() {
		mutated._indexes.lookup.should.eql( [ 10, 40 ] );
	} );

	it( 'should get key by index', function() {
		keys.length.should.equal( 2 );
		keys.sort().should.eql( [ 'test-key-1', 'test-key-2' ] );
	} );

	it( 'should fetch documents by keys', function() {
		list1.length.should.equal( 2 );
	} );

	it( 'should fetch documents by index', function() {
		list2.length.should.equal( 2 );
	} );

	it( 'should create siblings on conflicting puts', function() {
		siblings.length.should.equal( 2 );
		siblings[ 0 ].answer.should.equal( 'nope' );
		siblings[ 1 ].answer.should.equal( 'yarp' );
	} );

	it( 'should resolve document when putting with a vector clock', function() {
		resolved.answer.should.equal( 'yarp' );
	} );

	after( function() {
		riak.close();
	} );

} );