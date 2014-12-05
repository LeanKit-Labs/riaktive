var should = require( 'should' );
var connect = require( '../src/connection.js' );
var config = require( 'configya' )( './config.json', { riak: { server: 'ubuntu' } } );

describe( 'when connecting', function() {
	
	describe( 'with default endpoints passed', function() {
		var riak, pong;

		before( function( done ) {
			riak = connect( { host: config.riak.server } );
			riak.ping()
				.then( function() {
					pong = true;
					done();
				} );
		} );

		it( 'should get pong', function() {
			pong.should.be.true; // jshint ignore:line
		} );

		after( function() {
			riak.close();
		} );
	} );

	describe( 'when cycling through endpoints', function() {
		var riak, pong;
		before( function( done ) {
			riak = connect( [
				{ host: 'busted', timeout: 200 },
				{ host: config.riak.server }
			] );
			riak.ping()
				.then( function() {
					pong = true;
					done();
				} );
		} );

		it( 'should get pong', function() {
			pong.should.be.true; //jshint ignore:line
		} );

		after( function() {
			riak.close();
		} );
	} );

	describe( 'when unable to connect within limit', function() {
		var riak, pong;
		before( function( done ) {
			riak = connect( {
				nodes: [
					{ host: 'busted', timeout: 200 },
					{ host: 'hurp', timeout: 200 },
					{ host: 'derp', timeout: 200 },
					{ host: 'terp', timeout: 200 },
				],
				retries: 3,
				failed: function() {
					done();
				}
			} );
			riak.ping()
				.then( function() {
					pong = true;
				} );
		} );

		it( 'should not get pong', function() {
			should( pong ).not.exist; //jshint ignore:line
		} );

		after( function() {
			riak.close();
		} );
	} );

	describe( 'when failed and reset', function() {
		var riak, pong;
		before( function( done ) {
			this.timeout( 5000 );
			riak = connect( {
				nodes: [
					{ host: 'busted', timeout: 200 },
					{ host: 'hurp', timeout: 200 },
					{ host: 'derp', timeout: 200 },
					{ host: 'ubuntu' },
				],
				retries: 1,
				failed: function() {
					riak.reset();
				}
			} );
			riak.ping()
				.then( function() {
					pong = true;
					done();
				} );
		} );

		it( 'should not get pong', function() {
			should( pong ).not.exist; //jshint ignore:line
		} );

		after( function() {
			riak.close();
		} );
	} );

} );