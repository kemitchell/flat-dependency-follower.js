An [npm registry follower][follower] that calculates (and recalculates)
flat package dependency manifests for each registry update. Users can
query the manifest of any package at any version at any CouchDB-style
update sequence number.

[follower]: https://github.com/npm/registry-follower-tutorial

**WARNING:  Highly experimental dilettante technology!  Any resemblance
to real, usable npm package dependency graphs is purely coincidental.**

Flat package manifests are shaped like:

```javascript
[
  // "Dependency Records"
  {
    name: String,
    version: String, // SemVer version or URL
    // Optional.  Indicates the package is a direct dependency.
    range: String, // SemVer range or URL
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

The package ships with a bin script that starts following the public
registry and serves data via HTTP.

Useful request paths include:

- `GET /package/$name/$version/$sequence`, where `/$sequence` is the
  server's current sequence by default.  Serves a flat dependency tree,
  if any, as JSON.

- `GET /sequence` serves the server's current sequence number.
