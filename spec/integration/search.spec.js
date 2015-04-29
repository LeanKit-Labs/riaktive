require( '../setup.js' );
var seq = require( 'when/sequence' );
var connect = require( '../../src/index.js' ).connect;
var config = require( 'configya' )( { file: './spec/config.json' } );

describe( 'with connection to solr and an indexed bucket', function() {
	var riak, bucket, index;

	describe( 'with nested documents', function() {
		var list = [];
		before( function( done ) {
			this.timeout( 6000 );
			riak = connect( { host: config.riak.server } );
			bucket = riak.bucket( 'testBucket1', { 'search_index': 'testBucket_index', schema: 'riaktive_schema', schemaPath: './spec/test_solr.xml' } );
			index = riak.index( 'testBucket_index' );
			seq( [ function() {
					return bucket.put( {
						id: 'one',
						name: 'Alex',
						children: [ { name: 'Dexter' } ]
					} );
				}, function() {
					bucket.put( {
						id: 'two',
						name: 'Ian',
						children: [ { name: 'Ty' }, { name: 'Michah' }, { name: 'Averie' } ]
					} );
				}, function() {
					setTimeout( function() {
						index.search( { 'children.name': 'averie' }, function( item ) {
							list.push( item );
						} )
							.then( function() {
								done();
							} );
					}, 2000 );
				}
			] );
		} );

		it( 'should return expected match', function() {
			list.length.should.equal( 1 );
			var match = list[ 0 ];
			match.name.should.equal( 'Ian' );
		} );

		after( function( done ) {
			seq( [ function() {
					return bucket.del( 'one' );
				}, function() {
					return bucket.del( 'two' );
				}
			] )
				.then( function() {
					done();
				} );
		} );
	} );

	describe( 'with query stats', function() {
		var result;
		before( function( done ) {
			this.timeout( 6000 );
			bucket.put( { id: 'four', name: 'Alex' } );
			bucket.put( { id: 'five', name: 'Ian' } );
			bucket.put( { id: 'six', name: 'Becca' } );

			setTimeout( function() {
				index.search( { 'name': '*' }, { start: 1, rows: 2 }, true )
					.then( null, function( /* err */ ) {
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
		} );

		it( 'should show total docs available', function() {
			result.total.should.equal( 3 );
		} );

		it( 'should show correct start', function() {
			result.start.should.equal( 1 );
		} );

		it( 'should show query max score', function() {
			result.maxScore.should.ok; //jshint ignore:line
		} );

		it( 'should show query duration', function() {
			result.qTime.should.be.ok; //jshint ignore:line
		} );

		after( function( done ) {
			seq( [ function() {
					return bucket.del( 'four' );
				}, function() {
					return bucket.del( 'five' );
				}, function() {
					return bucket.del( 'six' );
				}
			] )
				.then( function() {
					done();
				} );
		} );
	} );

	describe( 'with a sorted query', function() {
		var result;
		before( function( done ) {
			this.timeout( 6000 );
			bucket.put( { id: 'seven', name: 'Fred', age: 23 } );
			bucket.put( { id: 'eight', name: 'Sally', age: 35 } );
			bucket.put( { id: 'nine', name: 'Becca', age: 36 } );

			setTimeout( function() {
				index.search( { 'name': '*' }, { sort: { age: 'asc' } }, true )
					.then( null, function( /* err */ ) {
						done();
					} )
					.done( function( res ) {
						result = res;
						done();
					} );
			}, 2000 );
		} );

		it( 'should sort correctly', function() {
			result.docs.length.should.equal( 3 );
			_.pluck( result.docs, 'name' )
				.should.eql( [ 'Fred', 'Sally', 'Becca' ] );
		} );

		after( function( done ) {
			seq( [ function() {
					return bucket.del( 'seven' );
				}, function() {
					return bucket.del( 'eight' );
				}, function() {
					return bucket.del( 'nine' );
				}
			] )
				.then( function() {
					done();
				} );
		} );
	} );

	describe( 'with search query errors', function() {
		var error;
		before( function( done ) {
			riak = connect( { host: config.riak.server } );
			bucket = riak.bucket( 'testBucket1', { 'search_index': 'testBucket_index', schema: 'riaktive_schema', schemaPath: './spec/test_solr.xml' } );
			index = riak.index( 'testBucket_index' );
			index.search( { name: 'this has spaces but is not enclosed in quotes' } )
				.then( null, function( err ) {
					error = err;
					done();
				} );
		} );

		it( 'should return solrError message', function() {
			error.name.should.equal( 'SolrError' );
		} );
	} );
} );
