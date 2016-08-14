An [npm registry follower][follower] that calculates (and recalculates)
flat package dependency manifests for each registry update. Users can
query the manifest of any package at any version at any CouchDB-style
update sequence number.

[follower]: https://github.com/npm/registry-follower-tutorial

## Flat Package Trees?

**WARNING:  Highly experimental dilettante technology!  Any resemblance
to real, usable npm package dependency graphs is purely coincidental.**

Flat package manifests are shaped like:

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

Flat package trees differ from graphs produced by either npm version
2 or npm version 3 in a number of ways.  Some of them we know about:

1.  Flat package tree resolution is "aggressive":  Version ranges
    always resolve to the highest-available version of the dependency
    that satisfies the range.  This is in contrast to both npm version
    2 and npm version 3, which will accept or intentionally find a
    lower version to avoid multiple versions of the same package.

2.  Flat package trees do not resolve Git, tarball, path, or other
    non-SemVer-range dependencies.

## HTTP Server

The package ships with a bin script that starts following the public
registry and serves data via HTTP.

Useful request paths include:

- `GET /package/$name/$version/$sequence`, where `/$sequence` is the
  server's current sequence by default.  Serves a flat dependency tree,
  if any, as JSON.

- `GET /sequence` serves the server's current sequence number.
