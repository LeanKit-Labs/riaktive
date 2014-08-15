var solrClient = require( 'solr-client' );
var when = require( 'when' );
var _ = require( 'lodash' );
var http = require( 'http' );
var createApi = require( './riak.js' ).createIndex;
var SolrError = require('../node_modules/solr-client/lib/error/solr-error.js');

function createClient( node, index ) {
	var solr = solrClient.createClient( {
		host: node.host,
		port: node.http || 8098,
		core: index,
		path: '/search/query'
	} );
	patchSolr( solr );
	return solr;
}

function createQuery( solr, body, params ) {
	var query = solr.createQuery().set().q( body ),
			useEdis = false;

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
			if( useEdis ) {
				query = query.edismax();
			}
		}
	return query;
}

function createResponseHandle( callback ) {
	return function( res ) {
			var buffer = '';
			var err = null;
			res.on( 'data', function( chunk ) {
				buffer += chunk;
			} );

			res.on( 'end', function() {
				if ( res.statusCode !== 200 ) {
					err = new SolrError( res.statusCode, buffer );
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
			id: [ d._yz_rb, d._yz_rk ].join( ':' ),
			bucket: d._yz_rb,
			key: d._yz_rk
		};
	} );
}

function patchSolr( solr ) { // jshint ignore:line
	// this is kinda terrible, but Riak's URL doesn't conform to SOLR's :|
	// I figure patching vs. rolling our own solr client is the lesser of two evils.
	solr.search = function( query, callback ) {
		var self = this;
		// Allow to be more flexible allow query to be a string and not only a Query object
		var parameters = query.build ? query.build() : query;
		this.options.fullPath = [ this.options.path, this.options.core, '?' + parameters + '&wt=json' ]
			.filter( function( element ) {
				if ( element ) {
					return true;
				}
				return false;
			} ).join( '/' );
		queryRequest( this.options, callback );
		return self;
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
			'authorization': params.authorization
		};
		options.headers = headers;
	}

	var callbackResponse = createResponseHandle( callback );
	var request = http.get( options, callbackResponse );

	request.on( 'error', function( err ) {
		if ( callback ) {
			callback( err, null );
		}
	} );
}

function search( riak, solr, body, params, includeStats ) {
	return when.promise( function( resolve, reject, notify ) {
		var query = createQuery( solr, body, params );
		solr.search( query, function( err, result ) {
			if ( err ) {
				reject( err );
			} else {
				var matches = parseResponse( result.response );
				var docs = [];
				riak.getByKeys( matches )
					.then( null, reject )
					.progress( function( doc ) {
						notify( doc );
						docs.push( doc );
					} )
					.done( function() {
						var response =  includeStats 
										? { docs: docs, 
											total:result.response.numFound,
											start: result.response.start,
											maxScore: result.response.maxScore,
											qTime: result.responseHeader.QTime
											 }
										: docs;
						resolve( response );
					});
			}
		} );
	} );
}

module.exports = function( riak, index ) {
	var api = createApi( riak );
	var solr = createClient( riak.getNode(), index );
	return { search: search.bind( undefined, api, solr ) };
};