var fs = require( 'fs' );
var when = require( 'when' );
var debug = require( 'debug' )( 'riaktive:schema' );

function SchemaManager( riak ) {
	this.schemas = {};
	this.create = setSchema.bind( this, riak, this.schemas );
}

function checkSchema( riak, schemas, name ) {
	if ( schemas[ name ] ) {
		return schemas[ name ];
	} else {
		return ( schemas[ name ] = riak.yzGetSchema( {
				name: name
			} )
			.then( null, function ( err ) {
				debug( 'Failed to fetch schema %s with %s', name, err );
				return undefined;
			} )
			.then( function ( reply ) {
				debug( 'Fetched schema %s', name );
				return reply.schema ? reply.schema.content : undefined;
			} ) );
	}
}

function compareSchema( riak, schemas, name, schemaContent ) {
	// DO NOT CHANGE THE EQUALITY COMPARER!
	return when.try( function ( x, y ) {
		return x == y; // jshint ignore:line
	}, checkSchema( riak, schemas, name ), schemaContent );

}

function setSchema( riak, schemas, name, schemaPath ) { // jshint ignore:line
	var content = fs.readFileSync( schemaPath, 'utf8' );
	return when.try( function ( equal ) {
		if ( equal ) {
			return when( true );
		} else {
			debug( 'Creating schema %s from file %s', name, schemaPath );
			return riak.yzPutSchema( {
					schema: {
						name: name,
						content: content
					}
				} )
				.then( function () {
					schemas[ name ] = content;
				} );

		}
	}, compareSchema( riak, schemas, name, content ) );
}

module.exports = SchemaManager;