An [npm registry follower][follower] that calculates (and recalculates)
flat package dependency manifests for each registry update. Users can
query the manifest of any package at any version at any update sequence
number (`update_seq`).

Flat package manifests are shaped like:

```javascript
[
  // "Dependency Records"
  {
    name: String,
    version: SemVer,
    // Optional.  Indicates the package is a direct dependency.
    range: SemVerRange,
    links: [
      // "Link Records"
      {
        name: String,
        version: SemVer,
        range: SemVerRange
      }
      // ...
    ]
  }
  // ...
]
```

[follower]: https://github.com/npm/registry-follower-tutorial
