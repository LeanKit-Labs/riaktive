require( 'should' );
var config = require( 'configya' )( './config.json' ),
	riak = require( '../src/riaktive.js' )( config ),
	_ = require( 'lodash' ),
	index;

describe( 'when connecting to riak', function() {
	before( function( done ) {
		riak.connect()
			.done( function() { done(); } );
	} );

	it( 'should be connected', function() {
		riak.connected.should.be.true;
	} );
} );

describe( 'with connection to solr and an indexed bucket', function () {
	var bucket, index;
	before( function( done ) {
		this.timeout( 20000 );
		riak.createBucket( 'testBucket', { search_index: 'testBucket_index', schema: 'riaktive_schema' } )
			.done( function( b ) {
				bucket = b;
				index = riak.getSearchIndex( 'testBucket_index' );
				done();
			} );
	} );

	it( 'should create the bucket successfully', function() {
		bucket.should.be.ok;
		index.should.be.ok;
	} );

	describe( 'with nested documents', function () {
		var list = [];
		before( function( done ) {
			this.timeout( 5000 );
			bucket.put( { id: 'one', name: 'Alex', children: [ { name: 'Dexter' } ] } );
			bucket.put( { id: 'two', name: 'Ian', children: [ { name: 'Ty' }, { name: 'Michah' }, { name: 'Averie' } ] } );

			setTimeout( function() {
				index.search( { 'children.name': 'averie' } )
					 .progress( function( item ) {
					 	console.log( item );
					 } )
					 .then( null, function( err ) {
					 	done();
					 } )
					 .done( function( all ) {
					 	list = all;
					 	done();
					 } );
			}, 2000 );
		} );

		it( 'should not poo', function() {
			list.length.should.equal( 1 );
		} );
	} );

	after( function() {
		bucket.del( 'one' );
		bucket.del( 'two' );
	} );

} );