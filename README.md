# riaktive
A Riak API abstraction built on riakpbc that aims for simplicity.

## Rationale
I like the riakpbc Node library is but using it requires detailed knowledge of Riak's sophisticated API. Our intention with riaktive is to provide a simple set of abstractions for the most common use cases.

Hopefully this library will ease Riak adoption.

## Features

 * Promises API
 * Connection pooling
 * Secondary indexing
 * Paging
 * 'Mutate' (fetch -> change -> put)
 * Search (via Riak's new Solr integration)
 * Pluggable key generation strategy

## Assumptions

 * riak 2.0 RC-1 or greater
 * levelDB storage backend
 * pbc interface support only
 * allow siblings (allow_mult = true)

### levelDB
levelDB is currently the simplest way to solve for certain common data access patterns (like paging). The small performance trade-off is preferable to resolving hard problems outside the database.

### Allow Siblings
This library defaults buckets to `allow_mult=true` on creation. Stick with the default, don't play last-write-wins roulette :)

## Quick Start

```javascript
var riaktive = require( 'riaktive' );

// defaults to 127.0.0.1 at port 8087 with http port at 8098 for Solr queries
var riak = riaktive.connect();

// creates a bucket with custom schema - index name defaults to bucketName_index
var bucket = riak.bucket( 'mahbucket', { schema: 'cat_schema', schemaPath: './spec/solr_cat.xml' } );

var doc = {
	name: 'kitteh',
	abilities: [ 'mew', 'purr', 'keyboard flop' ],
	type: 'felis catus'
};
var key;

// put the doc into Riak with a auto-generated id indexed by type and name
// note: you can always access the bucket via property
riak.mahbucket.put( doc, { type: doc.type, name: doc.name } )
	.then( null, function( err ) { /* handle failure */ } )
	.then( function( id ) { key = id; } );

// retrieve the document by key
bucket.get( key )
	.then( function( doc ) { // our cat document } );

var list1 = [];
var list2 = [];


// optional progress callbacks get called as
// each record comes back from Riak
function onDoc1( doc ) {
	list1.push( doc );
}

// retrieve all documents with the name 'kitteh'
bucket.getByIndex( 'name', 'kitteh', onDoc )
	.then( function( results ) {
		// results contains metadata as well as
		// `keys` and `docs` properties
	} );

// retrieve all documents of the type 'felis catus'
bucket.getByIndex( 'type', 'felis catus', function onDoc2( doc ) )
		list2.push( doc );
	} );

var searchResults = [];
// retrieve documents matching Solr search criteria
var index = riak.index( 'mahbucket_index' );
index.search( { name: 'kitteh' }, function onMatch( doc ) {
	searchResults.push( match );
} );
```

## Connectivity
Riaktive now supports multiple nodes and makes use of connection pooling. Each node is defined by a simple object with `host`, `port`, `http` and `timeout` properties. When any of these properties is not defined, default values will be used.

In addition to node definitions, you can provide `wait`, `retries` and `failed` parameters which control how `riaktive` behaves when connections fail.

### single node example - uses default wait and retries
```javascript
var riak = riaktive.connect( {
	host: 'localhost', // default host address
	port: 8097, // default PBC port
	http: 8098, // default HTTP port (for Solr requests)
	timeout: 2000, // default number of miliseconds riaktive will wait for a connection
	connections: 5 // default 5 live connections in the pool for this node
} );
```

### node list example - uses default wait and retries

```javascript
// in this example, all servers use default ports, all we need to supply is the host name
var riak = riaktive.connect( [
	{ host: 'riak-node1' },
	{ host: 'riak-node2' },
	{ host: 'riak-node3' },
	{ host: 'riak-node4' },
	{ host: 'riak-node5' }
] );
```

### full configuration
```javascript
var riak = riaktive.connect( {
	nodes: [
		{ host: 'riak-node1' },
		{ host: 'riak-node2' },
		{ host: 'riak-node3' },
		{ host: 'riak-node4' },
		{ host: 'riak-node5' }
	],
	wait: 5000, // the number of ms to wait between retrying nodes
	retries: 5, // the number of retries allowed before treating the node as unreachable
	failed: function() { // what to do when retries are exhausted across all nodes }
} );
```

### Command deferral
All commands are delayed until:

 1. A connection exists
 1. Any schema file and bucket index have been created
 1. The bucket settings have been set

Since all commands return a promise, you don't have to take any additional steps. The upside is that your application's flow isn't determined by connectivity timing.

In the event that no connection is ever established, the promises will all be rejected.

### Reconnection limit
Once the attempts to connect to a node have failed a consecutive number of times beyond the retries limit, the node will be marked as unreachable and taken out of the connection pool rotation.

When retries have been exhausted across all nodes, any outstanding promises for API calls will be rejected and the `failed` callback (if provided) will get called.

### Resetting
Once a connection pool has shutdown due to all nodes passing their reconnection limit, the pool can be restarted by calling `reset`:

```javascript
riak.reset();
```

## Id strategies
By default, a random UUID will be used as a key if one is not provided. This should be avoided if at all possible. The best way to manage this is to provide an id strategy (like [`sliver`](https://github.com/LeanKit-Labs/sliver)) which will produce unique, sortable keys without coordination.

The id strategy is a simple function that takes no arguments and returns a string. The following example demonstrates using sliver's getId call:

```javascript
var riaktive = require( 'riaktive' );
var sliver = require( 'sliver' )();
riaktive.setIdStrategy( sliver.getId );
```

> 'sliver' generates 128 bit, base 62 encoded, k-ordered, lexicographically sortable ids.

## Get, Put, Mutate, Delete
These bucket operations should be straight-forward. The one exception is mutate. Riaktive provides this call for cases when you need to read, change and persist the changes and would like to avoid siblings.

> Note: concurrent mutation is still a problem. See [hashqueue](https://github.com/LeanKit-Labs) to limit local concurrency and [consul-locker] for distributed mutual exclusion.

### get( key )
Get retrieves a document by key.

```javascript
riak.bucketName.get( 'someId' )
	.then( function( doc ) {}, function( err ) {} );
```

### put( [key], doc, [indexes] )
Puts a new document either by specific `key` arg, `id` property or a generated id using the id strategy. Indexes are a hash of key-values that will be attached to the document. The result of the promise is the key of the document (useful for puts that are using generated ids).

> Secondary Indexes - when a document is put, the indexes provided will be the only ones present. There is no way to only add or remove indexes from previous versions of a document with the same key.

> !Siblings! - putting a document to the same key without the previous vclock indicates to Riak that your app hasn't seen the previous version. This causes Riak to store the new document alongside the original. When siblings exist, riaktive will return an array containing all the siblings vs. a single document.

```javascript
var docA = {
	...
};
var docB = {
	id: 'natural key'
};
var docC = {
	...
	_indexes: {
		indexA: 1,
		indexB: 'stuff'
	}
};
var indexes = {
	indexOne: 1,
	indexTwo: 'two'
}

// put using a custom key with indexes
riak.bucketName.put( 'someId', docA, indexes )
	.then( function( id ) {} )
	.then( null, function( err ) {} );

// put using an id property, no indexes
riak.bucketName.put( docB )
	.then( function( id ) {}, function( err ) {} );

// put with a generated id, indexes attached to the document
riak.bucketName.put( docC )
	.then( function( id ) {}, function( err ) {} );
```

### mutate( key, mutateFn )
Mutate exists to read a document, make a change to it and save it back to Riak without creating a sibling by accident.

The mutate function will be passed the document and is expected to return either the changed document or original document. Mutate will only save the document if changes were actually made as a result of the call.

> Remember: mutate does not prevent concurrent mutation of the same key. Use a project like [hashqueue](https://github.com/LeanKit-Labs/hashqueue) to prevent accidental sibling creation from concurrent changes.

```javascript
riak.bucketName.mutate( 'someKey', function( doc ) {
	doc.newProperty = 'look, a new property';
	doc.amount += 10; // just because
	return doc;
} )
.then( function( doc ) {
	// doc represents the changed document
} );
```

### del( key )
Delete the document by key. Keep in mind that even deletes in Riak are eventually consistent. Riaktive attempts to filter these out of all get operations.

```javascript
riak.bucketName.del( 'someKey' )
	.then( function { /* callback when done */ } );
```

## Secondary Indexes
Secondary Indexes can be supplied to the put call and are available during mutation via the `_indexes` property. Riaktive provides calls that will retrieve either keys or documents by an index.

### index
Riaktive manages the _bin, _int suffixes on index names for you so that you don't have to provide those when creating new indexes or getting by them. In addition, you can also use the `$key` index as a way to get a range of keys. (frequently used for paging)

### start and finish
In most cases, the smallest starting key value is `!` and the largest finish key is `~`. Keep in mind that the sort order for binary indexes is lexicographic.

If an exact match is desired, only supply the start property.

### limit and continuation
Provide a limit to prevent unbounded index searches across the cluster. When a limit is specified, the resulting promise will finalize with a continuation that can be passed to the next call in order to get the 'next page' of results.

	Note: if a continuation exists, it will be attached to each result

### parameter hash
Both calls support passing all arguments as a hash:

```javascript
// this parameters hash would get the first 10 keys for the bucket
{
	index: '$key',
	start: '!',
	finish: '~',
	limit: 10
	continuation: undefined
}
```

### result set
Both calls resolve to a hash with the following properties:

```javascript
// index, start, finish and limit will be whatever was passed to the call
{
	index:
	start:
	finish:
	limit:
	keys: // the resulting keys
	docs: // matching docs - only gets set on the getByIndex call
	continuation: // only applies if there are more keys after the current set
}
```

### getKeysByIndex( index, start|exactMatch, [finish], [limit], [continuation], [progress] )
Returns a promise that resolves to a metadata about the index query and a key list and will also invoke the optional progress callback if provided for each key found.

### getByIndex( index, start|exactMatch, [finish], [limit], [continuation], [progress] )
Returns a promise that resolves to the full documents (in addition to keys)

## Search (Solr)
Riaktive supports the ability to define a schema and index per bucket and then query a Solr index and return documents to the optional `progress` callback as they are retrieved. The promise will resolve to the search results and can optionally include statistics from Solr.

### Search schema
When defining a bucket, the search schema is defined with the following properties:
```javascript
{
	schema: 'schemaName',
	schemaPath: '/path/to/schema.xml'
}
```

Riaktive will check for an existing schema with contents matching the file at `schemaPath` and create the schema if it's missing.

### Index
An index is associated with a bucket by the `search_index` property. If a `schema` property is also included, riaktive will create or update the index if a match with the same schema name doesn't exist.

If a schema is specified without an index name, riaktive will create one named after the bucket with the suffix '_index'

### Searching
You can easily search an existing Solr index using the JSON representation of Solr queries.

As with buckets, the index must be defined first before accessing the index off the riaktive instance directly or via the returned variable:

#### search( query, [options], [progress] )
Sends a query to Solr with any optional arguments and invokes the progress callback (if provided) after each document is retrieved from Riak.

```javascript
var myBucketIndex = riak.index( 'mybucket_index' );

function onMatch( match ) {
	// do something with each matching document
}

myBucketIndex.search( { name: 'Waldo }, onMatch )
	.then( function( statistics ) { /* do something with stats or the list */ } );
```

## Logging
Riaktive uses whistlepunk for logging. To provide configuration to control logging, use `configureLogging`.

```javascript
var riaktive = require( 'riaktive' );
riaktive.configureLogging( {
	adapters: {
		stdOut: {
			level: 4
		}
	}
} );
```

## Missing
If you see promise here but are disappointed about the lack of support for the following list, feel free to contribute:

 * Riak CRDTs
 * Support for call-level read/write behavior (quorum, all, single)
 * Bucket types
