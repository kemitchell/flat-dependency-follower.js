An [npm registry follower][follower] that calculates and recalculates
dependency trees for every public package on each registry update.
Clients can fetch data structures describing the dependency tree of any
version of any package.

[follower]: https://github.com/npm/registry-follower-tutorial

Exports a pull-stream sink for consuming registry updates and a number
of functions for reading data from the resulting flat-file store.
Installs a server that follows the npm public registry and serves
results via HTTP.

## Flat Package Trees?

**WARNING:  Highly experimental dilettante technology!  Any resemblance
to real, usable npm package dependency trees is purely coincidental.**

Flat package manifests are [adjacency lists] shaped like:

```javascript
[
  // "Dependency Records"
  {
    name: String,
    version: String, // SemVer version or URL
    range: String, // Optional SemVer range or URL
                   // Indicates the package is a direct dependency.
    links: [
      // "Link Records"
      {
        name: String,
        version: String, // SemVer version or URL
        range: String // SemVer range or URL
      }
      // ...
    ]
  }
  // ...
]
```

[adjacency lists]: https://en.wikipedia.org/wiki/Adjacency_list

Flat package trees differ from graphs produced by recent versions
of the npm command-line interface in a number of ways.  A few we
know about:

1.  Flat package tree resolution is "aggressive":  Version ranges
    always resolve to the highest-available version of the dependency
    that satisfies the range.  This is in contrast to both npm version
    2 and npm version 3, which will accept or intentionally find a
    lower version to avoid multiple versions of the same package.

2.  Flat package trees do not resolve Git, tarball, path, or other
    non-SemVer-range dependencies.

There are probably others.

## HTTP Server

The package ships with a bin script that starts following the public
registry and serves data via HTTP.

Useful request paths include:

-  `GET /packages` streams known package names, separated by newlines.

-  `GET /packages/{name}` serves a JSON array of package versions known
    to the follower.

-  `GET /packages/{name}/{version}` serves a flat dependency tree,
   if any, as JSON, at the latest sequence number.

-  `GET /packages/{name}/{version}/{sequence}` serves a flat dependency
    tree, if any, as JSON, at the given sequence number.

-  `GET /sequence` serves the server's current sequence number.

-  `GET /behind` serves the difference between the sequence of the
   replication API and the server's current sequence number.

## Special Thanks

-  [Chris Dickinson](https://www.npmjs.com/~chrisdickinson)

-  [Mikola Lysenko](https://www.npmjs.com/~mikolalysenko)

-  [Rebecca Turner](https://www.npmjs.com/~iarna)

-  [Ashley Williams](https://www.npmjs.com/~ag_dubs)
