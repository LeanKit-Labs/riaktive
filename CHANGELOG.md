## 0.1.*

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