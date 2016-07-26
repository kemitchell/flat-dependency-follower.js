var Writable = require('stream').Writable
var asyncMap = require('async.map')
var find = require('array-find')
var inherits = require('util').inherits
var lexint = require('lexicographic-integer')
var mergeFlatTrees = require('merge-flat-package-trees')
var semver = require('semver')
var updateFlatTree = require('update-flat-package-tree')

module.exports = FlatDependencyFollower

// A Note on Terminology
//
// Throughout this package:
//
// - When A depends on B, A is the "dependent", B is the "dependency".
//
// - A "tree" is a flattish data structure listing the dependencies that
//   need to be installed and how they depend on one another.
//
// - A "range" is a node-semver range.
//
// - A "version" is a node-semver version.

function FlatDependencyFollower (levelup) {
  if (!(this instanceof FlatDependencyFollower)) {
    return new FlatDependencyFollower(levelup)
  }
  this._levelup = levelup
  this._sequence = 0
  Writable.call(this, {objectMode: true})
}

inherits(FlatDependencyFollower, Writable)

var prototype = FlatDependencyFollower.prototype

prototype._write = function (chunk, encoding, callback) {
  var self = this
  var updatedName = chunk.name
  var sequence = chunk.sequence

  // `_write` prepares and executes one large batch of LevelUP
  // operations to store computed information about the update and its
  // consequences.
  var batch = []

  function writeBatch () {
    batch.forEach(function (operation) {
      // Make operations put operations by default.
      operation.type = 'put'
      // Set a placeholder for key-only records.  These are
      // primarily used for indexing.
      if (!operation.hasOwnProperty('value')) {
        operation.value = ''
      }
    })
    self._levelup.batch(batch, function (error) {
      /* istanbul ignore if */
      if (error) {
        callback(error)
      } else {
        self._sequence = sequence
        callback()
      }
    })
  }

  // Iterate versions of the package in the update.
  Object.keys(chunk.versions).forEach(function (updatedVersion) {
    var ranges = chunk.versions[updatedVersion].dependencies

    // Store the raw `.dependencies` object for the package.
    batch.push({
      key: encodeKey(
        'ranges',
        updatedName,
        updatedVersion,
        packInteger(sequence)
      ),
      value: ranges
    })

    // Compute the flat package dependency manifest for the new package.
    self._treeFor(
      sequence, updatedName, updatedVersion, ranges,
      function (error, tree) {
        /* istanbul ignore if */
        if (error) {
          callback(error)
        } else {
          // Store the tree.
          batch.push({
            key: encodeKey(
              'tree',
              updatedName,
              packInteger(sequence),
              updatedVersion
            ),
            value: tree
          })

          // Store key-only index records.  These will be used to
          // determine that this package's tree needs to be updated when
          // new versions of any of its dependencies---direct or
          // indirect---come in later.
          tree.forEach(function (dependency) {
            var dependencyName = dependency.name
            var withRanges = []
            // Direct dependency range.
            if (dependencyName in ranges) {
              withRanges.push(ranges[dependencyName])
            }
            // Indirect dependency ranges.
            tree.forEach(function (otherDependency) {
              otherDependency.links.forEach(function (link) {
                if (link.name === dependencyName) {
                  var range = link.range
                  /* istanbul ignore else */
                  if (withRanges.indexOf(range) === -1) {
                    withRanges.push(range)
                  }
                }
              })
            })
            withRanges.forEach(function (range) {
              batch.push({
                key: encodeKey(
                  'dependent',
                  dependencyName,
                  packInteger(sequence),
                  range,
                  updatedName,
                  updatedVersion
                )
              })
            })
          })

          // Update trees for packages that directly and indirectly
          // depend on the updated package.
          self._findDependents(
            sequence, updatedName, updatedVersion,
            function (error, dependents) {
              /* istanbul ignore if */
              if (error) {
                callback(error)
              } else {
                // Generate an updated tree for each dependent.
                asyncMap(
                  dependents,
                  function (record, done) {
                    var dependent = record.dependent
                    // Find the most current tree for the package.
                    self._findTree(
                      dependent.name,
                      dependent.version,
                      sequence,
                      function (error, result) {
                        /* istanbul ignore if */
                        if (error) {
                          done(error)
                        } else {
                          // Create a tree with:
                          //
                          // 1. the update package
                          // 2. the updated package's dependencies
                          //
                          // and use it to update the existing tree for
                          // the dependent package.
                          var treeClone = clone(tree)
                          treeClone.push({
                            name: updatedName,
                            version: updatedVersion,
                            links: treeClone
                            .reduce(function (links, dependency) {
                              return dependency.range
                              ? links.concat({
                                name: dependency.name,
                                version: dependency.version,
                                range: dependency.range
                              })
                              : links
                            }, [])
                          })
                          // Demote direct dependencies to indirect
                          // dependencies.
                          treeClone.forEach(function (dependency) {
                            delete dependency.range
                          })
                          var updated = updateFlatTree(
                            result.tree,
                            updatedName,
                            updatedVersion,
                            treeClone
                          )
                          done(null, {
                            name: dependent.name,
                            version: dependent.version,
                            tree: updated
                          })
                        }
                      }
                    )
                  },
                  function (error, newDependentTrees) {
                    /* istanbul ignore if */
                    if (error) {
                      callback(error)
                    } else {
                      // Push put operations for new dependent trees.
                      newDependentTrees.forEach(function (record) {
                        batch.push({
                          key: encodeKey(
                            'tree',
                            record.name,
                            packInteger(sequence),
                            record.version
                          ),
                          value: record.tree
                        })
                      })
                      writeBatch()
                    }
                  }
                )
              }
            }
          )
        }
      }
    )
  })
}

// Generate a tree for a package, based on the `.dependencies` object in
// its `package.json`.
prototype._treeFor = function (
  sequence, name, version, ranges, callback
) {
  var self = this
  asyncMap(
    // Turn the Object mapping from package name to SemVer range into an
    // Array of Objects with name and range properties.
    Object.keys(ranges).map(function (dependencyName) {
      return {
        name: dependencyName,
        range: ranges[dependencyName]
      }
    }),
    // For each name-and-range pair...
    function (dependency, done) {
      // ...find the dependency tree for the highest version that
      // satisfies the range.
      self._findMaxSatisfying(
        sequence, dependency.name, dependency.range,
        function (error, result) {
          /* istanbul ignore if */
          if (error) {
            if (error.noSatisfying) {
              error.message = (
                error.message + ' for ' + name + '@' + version
              )
              error.dependent = {
                name: name,
                version: version
              }
              error.sequence = sequence
            }
            done(error)
          } else {
            done(null, result)
          }
        }
      )
    },
    // Once we have trees for dependencies...
    function (error, dependencyTrees) {
      /* istanbul ignore if */
      if (error) {
        callback(error)
      } else {
        // ...combine them to form a new tree.
        var combinedTree = []
        dependencyTrees.forEach(function (tree) {
          combinedTree = mergeFlatTrees(combinedTree, tree)
        })
        callback(null, combinedTree)
      }
    }
  )
}

var ZERO = packInteger(0)

// Find the tree for the highest package version that satisfies a given
// SemVer range.
prototype._findMaxSatisfying = function (
  sequence, name, range, callback
) {
  // Fetch all the trees for the package at the current sequence.
  this._findTrees(sequence, name, function (error, records) {
    /* istanbul ignore if */
    if (error) {
      callback(error)
    } else {
      // Find the tree that corresponds to the highest SemVer that
      // satisfies our target range.
      var versions = records.map(function (record) {
        return record.version
      })
      var max = semver.maxSatisfying(versions, range)
      // If there isn't a match, yield an informative error with
      // structured data about the failed query.
      if (max === null) {
        var satisfyingError = new Error(
          'no package satisfying ' + name + '@' + range
        )
        satisfyingError.noSatisfying = true
        satisfyingError.dependency = {
          name: name,
          range: range
        }
        callback(satisfyingError)
      // Have a tree for a package version that satisfied the range.
      } else {
        var matching = find(records, function (record) {
          return record.version === max
        })
        // Create a new tree with just a record for the top-level
        // package.  The new records links to all direct dependencies in
        // the tree.
        var treeWithDependency = [
          {
            name: name,
            version: max,
            range: range,
            // Link to all direct dependencies.
            links: matching.tree.reduce(function (links, dependency) {
              return dependency.range
              ? links.concat({
                name: dependency.name,
                version: dependency.version,
                range: dependency.range
              })
              : links
            }, [])
          }
        ]
        // Demote direct dependencies to indirect dependencies.
        matching.tree.forEach(function (dependency) {
          delete dependency.range
        })
        var completeTree = mergeFlatTrees(
          matching.tree, treeWithDependency
        )
        callback(null, completeTree)
      }
    }
  })
}

// Get a tree for a specific package and version at a specific sequence.
prototype._findTree = function (name, versions, sequence, callback) {
  this._findTrees(sequence, name, function (error, trees) {
    /* istanbul ignore if */
    if (error) {
      callback(error)
    } else {
      callback(null, trees[0])
    }
  })
}

// Find all stored trees for a package at or before a given sequence.
prototype._findTrees = function (sequence, name, callback) {
  var matches = []
  this._levelup.createReadStream({
    gt: encodeKey('tree', name, ZERO, ''),
    lt: encodeKey('tree', name, packInteger(sequence), '~'),
    reverse: true
  })
  .once('error', /* istanbul ignore next */ function (error) {
    callback(error)
  })
  .on('data', function (data) {
    var decoded = decodeKey(data.key)
    matches.push({
      version: decoded[3],
      sequence: unpackInteger(decoded[2]),
      tree: data.value
    })
  })
  .once('end', function () {
    callback(null, matches)
  })
}

// Use key-only index records to find all direct and indirect dependents
// on a specific version of a specific package at or before a given
// sequence number.
prototype._findDependents = function (
  sequence, name, version, callback
) {
  var matches = []
  this._levelup.createReadStream({
    // Encode the low LevelUP key with an empty string suffix so
    // `encodeKey` will append the component separator, a slash.
    gt: encodeKey('dependent', name, ZERO, ''),
    // LevelUP key components are URI-encoded ASCII, so the tilde
    // character is high.
    lt: encodeKey('dependent', name, packInteger(sequence), '~'),
    keys: true,
    // There are no meaningful values, so we can skip them.
    values: false
  })
  .once('error', /* istanbul ignore next */ function (error) {
    callback(error)
  })
  .on('data', function decodeLevelUPKey (key) {
    var decoded = decodeKey(key)
    matches.push({
      sequence: decoded[2],
      dependency: {
        name: decoded[1],
        range: decoded[3]
      },
      dependent: {
        name: decoded[4],
        version: decoded[5]
      }
    })
  })
  .once('end', function filterSatisfyingVersions () {
    callback(null, matches.filter(function (match) {
      return semver.satisfies(version, match.dependency.range)
    }))
  })
}

// Public API

// Get the flat dependency graph for a package and version at a specific
// sequence number.
prototype.query = function (name, version, sequence, callback) {
  this._findTrees(sequence, name, function (error, matches) {
    /* istanbul ignore if */
    if (error) {
      callback(error)
    } else {
      var match = find(matches, function (match) {
        return match.version === version
      })
      if (match) {
        callback(null, match.tree, match.sequence)
      } else {
        callback(null, null, null)
      }
    }
  })
}

// Get the last-processed sequence number.
prototype.sequence = function () {
  return this._sequence
}

// LevelUP String Encoding Helper Functions

function encodeKey (/* variadic */) {
  return Array.prototype.slice.call(arguments)
  .map(encodeURIComponent)
  .join('/')
}

function decodeKey (key) {
  return key
  .split('/')
  .map(decodeURIComponent)
}

function packInteger (integer) {
  return lexint.pack(integer, 'hex')
}

function unpackInteger (string) {
  return lexint.unpack(string, 'hex')
}

// Helper Functions

function clone (argument) {
  return JSON.parse(JSON.stringify(argument))
}
