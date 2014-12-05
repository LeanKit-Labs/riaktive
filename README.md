# riaktive
A Riak API abstraction built on riakpbc that aims for simplicity.

## Rationale
While I really like how clean and well designed the riakpbc Node library is, using it requires detailed knowledge of Riak's rather sophisticated API. It also expects that you'll manage the connection and any setup in your consuming code. This doesn't seem like the sort of thing that should be hanging around in applications.

I also hope that putting a simpler API out there will encourage folks to give Riak a try.

## Features

 * Promises API
 * Connection pooling
 * Secondary indexing
 * Paging
 * 'Mutate' (fetch -> change -> put)
 * Search (via Riak's new Solr integration)
 * Flake-Id generation (128 bit, lexicographically sortable keys)

## Assumptions

 * riak 2.0 RC-1 or greater
 * levelDB storage backend
 * pbc interface support only
 * allow siblings (allow_mult = true)
 * default SOLR schema

### levelDB
levelDB is currently the best way I know to solve for certain common data access patterns (like paging). Depending on how you're using the secondary indexes, it's possible you could see some small performance hits. Give how fast Riak is, my guess is that other areas of your app will be bottlenecks long before Riak is.

### Allow Siblings
This library defaults buckets to `allow_mult=true` on creation. While you could change this, you should not play last-write-wins roulette :)

### Default Schema
If you don't specify a schema file, riaktive will provide one with the closest thing to sensible defaults that we've found to work to date. YMMV - if you know you want to use something different, provide your own schema file.

## Quick Start

```javascript
var riaktive = require( 'riaktive' );

var riak = riaktive.connect(); // defaults to 127.0.0.1 at port 8087 with http port at 8098 for Solr queries
var bucket = riak.bucket( 'mahbucket' ); // creates a bucket with default options

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
// retrieve all documents with the name 'kitteh'
// results are only provided via the progress call
bucket.getByIndex( 'name', 'kitteh' )
	.progress( function( match ) {
		list1.push( match );
	} );

// retrieve all documents of the type 'felis catus'
bucket.getByIndex( 'type', 'felis catus' )
	.progress( function( match ) {
		list2.push( match );
	} );

var searchResults = [];
// retrieve documents matching Solr search criteria
// note: the default Solr schema riaktive provides indexes 'name' properties
// also: default search index is {bucketName}_index
var index = riak.index( 'mahbucket_index' );
index.search( { name: 'kitteh' } )
	.progress( function( match ) {
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
	http: 8098 // default HTTP port (for Solr requests),
	timeout: 2000 // default number of miliseconds riaktive will wait for a connection
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

## Flake-ids
Riak will generate a 128 bit, base 62 encoded, k-ordered, lexicographically sortable ids. The only prerequisite for this feature is providing a **globally unique** node id for your service instance. as part of the connect call:

```javascript
riak.connect( {
	nodes: [
		...
	],
	nodeId: 'this-must-be-globally-unique'
}
```

	If you don't provide a nodeId, riaktive will base its nodeId on a randomly generated uuid which is then murmurhashed. There is some potential for collision (but it's extremely low). If you want to be certain, you'll need a strategy for acquiring unique nodeIds from a service.

## Get, Put, Mutate, Delete
These bucket operations should be straight-forward. The one exception is mutate. Riaktive provides this call for cases when you need to read, change and persist the changes without creating siblings*.

### get( key )
Get retrieves a document by key.

```javascript
riak.bucketName.get( 'someId' )
	.then( function( doc ) {} )
	.then( null, function( err ) {} );
```

### put( key, doc, indexes ), put( doc, indexes )
Puts a new document either by specific `key` arg, `id` property or a generated flake id. Indexes are a hash of key-values that will be attached to the document. The result of the promise is the key of the document (useful for puts that are using generated flake ids).

> Secondary Indexes - when a document is put, the indexes provided will be the only ones present. There is no way to only add or remove indexes from previous versions of a document with the same key.

```javascript
var docA = {
	...	
};
var docB = {
	id: 'natural key'
};
var indexes = {
	indexOne: 1, 
	indexTwo: 'two'
}

// put by a generated key
riak.bucketName.put( 'someId', docA )
	.then( function( id ) {} )
	.then( null, function( err ) {} );

// put by an id property
riak.bucketName.put( docB )
	.then( function( id ) {} )
	.then( null, function( err ) {} );
```

> Siblings - putting a document to the same key without the previous vclock indicates to Riak that your app hasn't seen the previous version. This causes Riak to store the new document alongside the original. When siblings exist, riaktive will return an array containing all the siblings vs. a single document.

### mutate( key, mutateFn )
Mutate exists only to support cases when you need to read a document, make a change to it and save it back to Riak without creating a sibling by accident.

The mutate function will be passed the document and is expected to return the changed document.

```javascript
riak.bucketName.mutate( 'someKey', function( doc ) {
	doc.newProperty = 'look, a new property';
	doc.amount += 10; // just because
	return doc;
} );
```

### del( key )
Delete the document by key. Keep in mind that even deletes in Riak are eventually consistent. Riaktive attempts to filter these out of all get operations.

```javascript
riak.bucketName.del( 'someKey' )
	.then( function { /* callback when done */ } );
```

## Secondary Indexes
As you've seen, you can supply secondary indexes with the put call. Riaktive provides two calls for retrieving keys or documents by index.

### index
Riaktive manages the _bin, _int suffixes on index names for you so that you don't have to provide those when creating new indexes or getting by them. In addition, you can also use the `$key` index as a way to get a range of keys. (frequently used for paging)

### start and finish
In most cases, the smallest starting key value is `!` and the largest finish key is `~`. Keep in mind that the sort order for binary indexes is lexicographic.

If you are looking for an exact match, rather than a range, supply only a start value.

### limit and continuation
Providing a limit to prevent unbounded index searches across your cluster. When providing a limit, the resulting promise will finalize with a continuation that can be passed for the next call in order to get the 'next page' of results.

	Note: if a continuation exists, it will also be attached to each result

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

### getKeysByIndex( index, start|exactMatch, [finish], [limit], [continuation] )
This call will return the keys to the `.progress` callback of the resulting promise.

### getByIndex( index, start|exactMatch, [finish], [limit], [continuation] )
This call retrieves the documents (instead of only the keys) and passes each one as soon as it is retrieved to the `.progress` callback.

## Search (Solr)
Riaktive supports the ability to define a schema and index per bucket and then query a Solr index and return documents to the result promise's `.progress` callback as they are retrieved. The promise's `.then` callback will also receive the search statistics from Solr on successful calls.

### Search schema
When defining a bucket, the search schema is defined with the following properties:
```javascript
{
	schema: 'schemaName',
	schemaPath: '/path/to/schema.xml'
}
```

Riaktive will check for an existing schema with contents matching the file at `schemaPath` and create the schema if it's missing.

As mentioned before, riaktive provides a default schema file that it uploads to Riak named 'riaktive_schema'.

### Index
An index is associated with a bucket by the `search_index` property. If a `schema` property is also included, riaktive will create or update the index if a match with the same schema name doesn't exist.

If you don't specify an index, riaktive will create one named after the bucket with the suffix `_index` that uses the 'riaktive_schema' mentioned earlier.

### Searching
You can easily search an existing Solr index using the JSON representation of Solr queries.

Just like with buckets, you need to define the index first and then either access the index off the riaktive instance directly or via the returned variable:

```javascript
var myBucketIndex = riak.index( 'mybucket_index' );
myBucketIndex.search( { name: 'Waldo } )
	.progress( function( matchingDoc ) { /* do something with match */ } )
	.then( function( statistics ) { /* do something with stats */ } );
```

## Roadmap
 * Spin up multiple connections per node in the connection pool when demand exceeds available connections
 * Decrease the number of connections per node in the connection pool if demand decreases

## Missing
If you see promise here but are disappointed about the lack of support for the following list, feel free to contribute:

 * Riak CRDTs
 * Support for call-level read/write behavior (quorum, all, single)
 * Bucket types