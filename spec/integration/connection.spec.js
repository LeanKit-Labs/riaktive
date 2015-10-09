require( "../setup.js" );
var connect = require( "../../src/index.js" ).connect;
var config = require( "configya" )( { file: "./spec/config.json" } );

describe( "when connecting", function() {
	describe( "with default endpoints passed", function() {
		var riak;

		before( function() {
			riak = connect( { host: config.riak.server, timeout: 3000 } );
		} );

		it( "should get pong", function() {
			return riak.ping()
				.should.eventually.eql( {} );
		} );

		after( function() {
			riak.close();
		} );
	} );

	describe( "when cycling through endpoints", function() {
		var riak;
		before( function() {
			this.timeout( 5000 );
			riak = connect( {
				nodes: [
					{ host: "busted" },
					{ host: config.riak.server }
				] }
			);
		} );

		it( "should get pong", function() {
			return riak.ping()
				.should.eventually.eql( {} );
		} );

		after( function() {
			riak.close();
		} );
	} );

	describe( "when unable to connect within limit", function() {
		var riak, failed;
		before( function() {
			riak = connect( {
				nodes: [
					{ host: "busted", timeout: 200 },
					{ host: "hurp", timeout: 200 },
					{ host: "derp", timeout: 200 },
					{ host: "terp", timeout: 200 }
				],
				retries: 1,
				wait: 200,
				failed: function() {
					failed = true;
				}
			} );
		} );

		it( "should fail to get a pong", function() {
			return riak.ping().should.be.rejectedWith( "All nodes were unreachable." );
		} );

		it( "should call failed handler", function() {
			expect( failed ).to.be.true; // jshint ignore:line
		} );

		after( function() {
			riak.close();
		} );
	} );

	describe( "when failed", function() {
		var riak, failed, nodes;
		before( function() {
			nodes = [
				{ host: "busted", timeout: 200 },
				{ host: "hurp", timeout: 200 },
				{ host: "derp", timeout: 200 }
			];
			riak = connect( {
				nodes: nodes,
				retries: 1,
				wait: 200,
				failed: function() {
					failed = true;
				}
			} );
		} );

		it( "should fail initially", function() {
			return riak.ping().should.be.rejectedWith( "All nodes were unreachable." );
		} );

		it( "should call failed handler", function() {
			expect( failed ).to.be.true; // jshint ignore:line
		} );

		describe( "after resetting", function() {
			before( function() {
				riak.pool.addNode( { host: "localhost" } );
				riak.reset();
			} );

			it( "should get a pong", function() {
				return riak.ping().should.eventually.eql( {} );
			} );
		} );

		after( function() {
			riak.close();
		} );
	} );
} );
