## 0.2.*

### Current

 * Replace sliver with pluggable idStrategy approach
 * Update mutate to always return document (changed or not)
 * Make _indexes available during mutate
 * Add default timeout to ensure promises reject on connection failure
 * Use whistlepunk for logging
 * Add ability to specific # of connections per node to start with in the pool
 * Bug fix - connection pool doesn't cycle connections per node correctly
 * Bug fix - correct how defaults are applied to node settings
 * Simplify and clean-up log entries
 * Update/Cleanup README

## 0.1.*

### prerelease 10
Update sliver version to include support for Node 0.12.*

### prerelease 9
Fix bug where calling bucket or index multiple times would declare the bucket/index every time.

### prerelease 8

 * Removed default schema
 * Improve implementation of mutate to only persist if changes occur - resolve to boolean to indicate a change
 * Fix broken test asserting that failed/reset works as expected
 * No longer remove '_indexes' property from documents on put
 * Rework build script to use biggulp
 * Replace should with chai

### prerelease 7

 * Compatibility with riakpbc 0.2.*
 * Introduced custom connection pool
 * Rejects API promises if no nodes can be reached
 * Better control over connection handling

### prerelease 4

 * Fixed breaking changes introduced by riakpbc
 * Fix bug where bucket aliases didn't get set correctly

### prerelease 3
Bug fixes for paging and search.

### prerelease 2
Added support for multi-valued secondary indexes.

### prerelease 1
Introduced a connection/pre-requisite monad to end callback madness around establishing a connection and bucket setup before being able to use the API.
