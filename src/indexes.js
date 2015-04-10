var when = require( 'when' );
var log = require( './log' )( 'indexes' );

var IndexManager = function( riak ) {
	this.indexes = {};
	this.create = setIndex.bind( undefined, riak, this.indexes );
};

function getIndex( riak, indexes, name ) {
	if ( indexes[ name ] ) {
		return indexes[ name ];
	} else {
		return ( indexes[ name ] = riak.yzGetIndex( {
			name: name
		} )
			.then( null, function( /* err */ ) {
				return {};
			} )
			.then( function( reply ) {
				return reply.index ? reply.index[ 0 ].schema : undefined;
			} ) );
	}
}

function compareIndex( riak, indexes, name, schema ) {
	return when.try( function( x ) {
		return x === schema;
	}, getIndex( riak, indexes, name ) );
}

function setIndex( riak, indexes, name, schema ) { // jshint ignore:line
	return when.try( function( equal ) {
		if ( equal ) {
			return when( false );
		} else {
			log.debug( 'Creating index "%s" with schema %s', name, schema );
			return riak.yzPutIndex( {
				index: {
					name: name,
					schema: schema
				}
			} )
				.then( function() {
					indexes[ name ] = schema;
					return ( true );
				} );
		}
	}, compareIndex( riak, indexes, name, schema ) );
}

module.exports = IndexManager;
