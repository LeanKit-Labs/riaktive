require( "../setup" );
var when = require( "when" );
//var machina = require("machina");
var noOp = function() {};

describe( "bucketFsm", function() {

	describe( "when started with no schema", function() {
		var stubs, readBucketFn, riak;
		var bucket, bucketFsm, api;
		before( function() {
			stubs = {
				"./indexes.js": function() {},
				"./schema.js": function() {}
			};

			bucketFsm = proxyquire( "../src/bucketFsm.js", stubs );
			api = { readBucket: noOp };
			readBucketFn = sinon.stub( api, "readBucket", function() {
				return when.resolve( {} );
			} );

			riak = {};
			function createBucket() {
				return api;
			}
			bucket = bucketFsm( "test1", {}, riak, createBucket );
		} );
		it( "calls readBucket on api", function() {
			readBucketFn.withArgs( riak, "test1" ).called.should.be.true;
		} );

	} );

} );
