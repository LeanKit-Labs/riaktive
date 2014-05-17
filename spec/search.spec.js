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
					 	//console.log( item );
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

		it( 'should return expected match', function() {
			list.length.should.equal( 1 );
			var match = list[ 0 ];
			match.name.should.equal( 'Ian' );
		} );

		after( function() {
			bucket.del( 'one' );
			bucket.del( 'two' );
		} );
	} );

	describe( 'with query stats', function() {
		var result;
		before( function( done ) {
			this.timeout( 5000 );
			bucket.put( { id: 'one', name: 'Alex' } );
			bucket.put( { id: 'two', name: 'Ian'  } );
			bucket.put( { id: 'three', name: 'Becca' });

			setTimeout( function() {
				index.search( { 'name': '*' }, { start:1, rows:2 }, true )
					 .progress( function( item ) {
					 	//console.log( item );
					 } )
					 .then( null, function( err ) {
					 	done();
					 } )
					 .done( function( res ) {
					 	result = res;
					 	done();
					 } );
			}, 2000 );
		} );

		it( 'should return expected results', function() {
			result.docs.length.should.equal( 2 );
			var match = result.docs[ 1 ];
			match.name.should.equal( 'Becca' );
		} );

		it( 'should show total docs available', function() {
			result.total.should.equal( 3 );
		} );

		it( 'should show correct start', function() {
			result.start.should.equal( 1 );
		});

		it( 'should show query max score', function() {
			result.maxScore.should.ok;
		});

		it( 'should show query duration', function() {
			result.qTime.should.be.ok;
		});

		after( function() {
			bucket.del( 'one' );
			bucket.del( 'two' );
			bucket.del( 'three' );
		} );
	} );

	describe( 'with search query errors', function() {
		var error;
		before( function( done ) {

			index.search( { name: 'this has spaces but is not enclosed in quotes' } 	 )
				 .progress( function( item ) {
				 	//console.log( item );
				 } )
				 .then( null, function( err ) {
				 	error = err;
				 	done();
				 } )
				 .done( function( res ) {
				 	if( res ) {
				 		done();
				 	}
				 } );
		});
		
		it( 'should return solrError message', function() {
			error.name.should.equal( 'SolrError' );
		});
	})
} );