var solrClient = require( "solr-client" );
var when = require( "when" );
var _ = require( "lodash" );
var http = require( "http" );
var createApi = require( "./riak.js" ).createIndex;
var SolrError = require( "../node_modules/solr-client/lib/error/solr-error.js" );
var log = require( "./log" )( "riaktive.search" );

function createClient( node, index ) {
	var solr = solrClient.createClient( {
		host: node.host,
		port: node.http,
		core: index,
		path: "/search/query"
	} );
	patchSolr( solr );
	return solr;
}

function createQuery( solr, body, params ) {
	var query = solr.createQuery().set().q( body );
	var useEdis = false;

	if ( params ) {
		if ( params.start ) {
			query = query.start( params.start );
		}
		if ( params.rows ) {
			query = query.rows( params.rows );
		}
		if ( params.sort ) {
			query = query.sort( params.sort );
		}
		if ( params.factors ) {
			useEdis = true;
			query = query.qf( params.factors );
		}
		if ( params.minimumMatch ) {
			useEdis = true;
			query = query.mm( params.minimumMatch );
		}
		if ( params.boost ) {
			useEdis = true;
			query = query.boost( params.boost );
		}
		if ( useEdis ) {
			query = query.edismax();
		}
	}
	return query;
}

function createResponseHandle( callback ) {
	return function( res ) {
		var buffer = "";
		var err = null;
		res.on( "data", function( chunk ) {
			buffer += chunk;
		} );

		res.on( "end", function() {
			if ( res.statusCode !== 200 ) {
				err = new SolrError( { headers: {} }, res, buffer );
				if ( callback ) {
					callback( err, null );
				}
			} else {
				var data;
				try {
					data = JSON.parse( buffer );
				} catch ( error ) {
					err = error;
				} finally {
					if ( callback ) {
						callback( err, data );
					}
				}
			}
		} );
	};
}

function parseResponse( response ) {
	return _.map( response.docs, function( d ) {
		return {
			id: [ d._yz_rb, d._yz_rk ].join( ":" ),
			bucket: d._yz_rb,
			key: d._yz_rk
		};
	} );
}

function patchSolr( solr ) { // jshint ignore:line
	// this is kinda terrible, but Riak"s URL doesn"t conform to SOLR"s :|
	// I figure patching vs. rolling our own solr client is the lesser of two evils.
	solr.search = function( query, callback ) {
		var that = this;
		// Allow to be more flexible allow query to be a string and not only a Query object
		var parameters = query.build ? query.build() : query;
		this.options.fullPath = [ this.options.path, this.options.core, "?" + parameters + "&wt=json" ]
			.filter( function( element ) {
				if ( element ) {
					return true;
				}
				return false;
			} ).join( "/" );
		queryRequest( this.options, callback );
		return that;
	};
}

function queryRequest( params, callback ) { // jshint ignore:line
	var options = {
		host: params.host,
		port: params.port,
		path: params.fullPath
	};

	if ( params.agent !== undefined ) {
		options.agent = params.agent;
	}

	if ( params.authorization ) {
		var headers = {
			authorization: params.authorization
		};
		options.headers = headers;
	}
	var request = http.get( options, createResponseHandle( callback ) );

	request.on( "error", function( err ) {
		if ( callback ) {
			callback( err, null );
		}
	} );
}

function search( riak, solr, index, body, params, includeStats, progress ) {
	return when.promise( function( resolve, reject ) {
		if ( _.isFunction( includeStats ) ) {
			progress = includeStats;
			includeStats = false;
		} else if ( _.isFunction( params ) ) {
			progress = params;
			params = undefined;
			includeStats = false;
		}
		var query = createQuery( solr, body, params );
		var notify = progress || _.noop;
		solr.search( query, function( err, result ) {
			if ( err ) {
				log.error( "Searching index \"%s\" with query %j failed with %s",
					index,
					query,
					err.stack );
				reject( err );
			} else {
				var matches = parseResponse( result.response );
				var onDocs = function( docs ) {
					var response = includeStats ?
						{ keys: matches,
							docs: docs,
							total: result.response.numFound,
							start: result.response.start,
							maxScore: result.response.maxScore,
							qTime: result.responseHeader.QTime
						} : docs;
					resolve( response );
				};
				riak.getByKeys( matches, notify )
					.then( onDocs, reject );
			}
		} );
	} );
}

module.exports = function( riak, index ) {
	var api = createApi( riak );
	var solr = createClient( riak.getNode(), index );
	return { search: search.bind( undefined, api, solr, index ) };
};
