var solrClient = require( 'solr-client' ),
	when = require( 'when' ),
	_ = require( 'lodash' );

module.exports = function( config, riak ) {

	var Index = function( index ) {
		this.solr = solrClient.createClient( 
		{
			host: config.get( 'SOLR_HOST', 'ubuntu' ),
			port: config.get( 'SOLR_PORT', 8093 ),
			core: index,
			path: config.get( 'SOLR_PATH', '/solr' )
		} );
	};

	Index.prototype.search = function( body, params ) {
		return when.promise( function( resolve, reject, notify ) {
			var query = this.solr.createQuery().q( body );
			if( params ) {
				if( params.start ) {
					query = query.start( params.start );
				}
				if( params.rows ) {
					query = query.rows( params.rows );
				}
				if( params.factors ) {
					query = query.qf( params.factors );
				}
				if( params.minimumMatch ) {
					query = query.mm( params.minimumMatch );
				}
				if( !params.strict ) {
					query = query.edismax();
				}
				if( params.boost ) {
					query = query.boost( params.boost );
				}
			}
			
			this.solr.search( query, function( err, result ){
				if( err ) {
					reject( err );
				} else {
					var matches = _.map( result.response.docs, function( d ) {
						return { 
							id: [ d._yz_rb, d._yz_rk ].join(':'),
							bucket: d._yz_rb,
							key: d._yz_rk
						};
					} );
					matches = _.uniq( matches, function( match ) { return match.id } );
					riak.getByKeys( matches )
						.then( null, reject )
						.progress( notify )
						.done( resolve );
				}
			}.bind( this ) );
		}.bind( this ) );
	};
	
	return Index;
};