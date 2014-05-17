var solrClient = require( 'solr-client' ),
	when = require( 'when' ),
	_ = require( 'lodash' ),
	http = require( 'http' ),
	SolrError = require('../node_modules/solr-client/lib/error/solr-error.js');

module.exports = function( config, riak ) {

	var queryRequest = function( params, callback ) {
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

		var callbackResponse = function( res ) {
			var buffer = '';
			var err = null;
			res.on( 'data', function( chunk ) {
				buffer += chunk;
			} );

			res.on( 'end', function() {
				if ( res.statusCode !== 200 ) {

					err = new SolrError( res.statusCode, buffer );
					if ( callback ) callback( err, null );
				} else {
					try {
						var data = JSON.parse( buffer );
					} catch ( error ) {
						err = error;
					} finally {
						if ( callback ) callback( err, data );
					}
				}
			} );
		}

		var request = http.get( options, callbackResponse );

		request.on( 'error', function( err ) {
			if ( callback ) callback( err, null );
		} );
	}

	var Index = function( index ) {
		this.solr = solrClient.createClient( {
			host: config.get( 'RIAK_SERVER', 'ubuntu' ),
			port: config.get( 'RIAK_HTTP', 8098 ),
			core: index,
			path: '/search'
		} );

		// this is kinda terrible, but Riak's URL doesn't conform to SOLR's :|
		// I figure patching vs. rolling our own solr client is the lesser of two evils.
		this.solr.search = function( query, callback ) {
			var self = this;
			// Allow to be more flexible allow query to be a string and not only a Query object
			var parameters = query.build ? query.build() : query;
			this.options.fullPath = [ this.options.path, this.options.core, '?' + parameters + '&wt=json' ]
				.filter( function( element ) {
					if ( element ) return true;
					return false;
				} )
				.join( '/' );;
			queryRequest( this.options, callback );
			return self;
		}
	};

	Index.prototype.search = function( body, params, includeStats ) {
		return when.promise( function( resolve, reject, notify ) {
			var query = this.solr.createQuery().set( 'wt=json' ).q( body ),
				useEdis = false;

			if ( params ) {
				if ( params.start ) {
					query = query.start( params.start );
				}
				if ( params.rows ) {
					query = query.rows( params.rows );
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

			this.solr.search( query, function( err, result ) {
				if ( err ) {
					reject( err );
				} else {
					var matches = _.map( result.response.docs, function( d ) {
						return {
							id: [ d._yz_rb, d._yz_rk ].join( ':' ),
							bucket: d._yz_rb,
							key: d._yz_rk
						};
					} );
					matches = _.uniq( matches, function( match ) {
						return match.id
					} );
					riak.getByKeys( matches )
						.then( null, reject )
						.progress( notify )
						.done( function( searchResult ) {
							var response =  includeStats 
											? { docs: searchResult, 
												total:result.response.numFound,
												start: result.response.start,
												maxScore: result.response.maxScore,
												qTime: result.responseHeader.QTime
												 }
											: searchResult;
							resolve( response );
						});
				}
			}.bind( this ) );
		}.bind( this ) );
	};

	return Index;
};