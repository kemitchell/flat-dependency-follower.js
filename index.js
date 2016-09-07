var Writable = require('stream').Writable
var asyncEach = require('async.each')
var asyncMap = require('async.map')
var deepEqual = require('deep-equal')
var ecb = require('ecb')
var inherits = require('util').inherits
var lexint = require('lexicographic-integer')
var mergeFlatTrees = require('merge-flat-package-trees')
var normalize = require('normalize-registry-metadata')
var pump = require('pump')
var runWaterfall = require('run-waterfall')
var semver = require('semver')
var sortFlatTree = require('sort-flat-package-tree')
var through = require('through2')
var to = require('flush-write-stream')
var updateFlatTree = require('update-flat-package-tree')

module.exports = FlatDependencyFollower

// A Note on Terminology
//
// Throughout this package:
//
// - An "update" is a CouchDB replication-style JSON blob received from
//   the npm public registry.
//
// - A "sequence number" is an integer `.seq` property of an update.
//
// - When A depends on B, A is the "dependent", B is the "dependency".
//
// - A "tree" is a flattish data structure listing the dependencies that
//   need to be installed and how they depend on one another.
//
// - A "range" is a node-semver range or URL.
//
// - A "version" is a node-semver version or URL.

// LevelUP Record Structure
//
// All LevelUP keys are formed by concatenating string components to
// create meaningful prefixes.  Components are encoded URI-style, with
// slashes and %-codes.  lexicographic-integer encodes sequence number
// integers to hex.
//
// Last Updates
//
//     update/{name} -> [{version, dependencies}, ...]
//
// Dependency Trees
//
//     tree/{name}/{sequence}/{version} -> Array
//
// These records store the precomputed flat package trees.  The prefix
// leads with sequence, rather than version, because Semantic Versions
// strings aren't lexicographically ordered.
//
//     pointer/{name}/{version}/{sequence}
//
// `prototype.query` uses these "pointer" keys to find the last tree
// record key for a package by sequence number.
//
// Dependency Relationships
//
//     dependency/{dependency}/{sequence}/{range}/{dependent}/{version}
//
// `prototype._findDependents` uses these keys to identify existing
// package trees that need to be updated.
var UPDATE_PREFIX = 'update'
var TREE_PREFIX = 'tree'
var POINTER_PREFIX = 'pointer'
var DEPENDENCY_PREFIX = 'dependency'

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
  var sequence = chunk.seq
  chunk = chunk.doc

  if (!validName(chunk.name) || !validVersions(chunk.versions)) {
    self._sequence = sequence
    self.emit('sequence', sequence)
    return callback()
  }

  normalize(chunk)
  var updatedName = chunk.name
  self.emit('updated', updatedName)

  function finish () {
    self._sequence = sequence
    self.emit('sequence', sequence)
    callback()
  }

  runWaterfall(
    [
      // Read the last saved update, which we will compare with the
      // current update to identify changed versions.
      function (done) {
        self._getLastUpdate(updatedName, done)
      },

      // Identify changed versions and process them.
      function (lastUpdate, done) {
        var versions = changedVersions(lastUpdate, chunk)
        self.emit('versions', versions)
        asyncEach(versions, function (version, done) {
          self._updateVersion(sequence, version, done)
        }, done)
      },

      // Overwrite the update record for this package, so we can compare
      // it to the next update for this package later.
      function (done) {
        self._putUpdate(chunk, done)
      }
    ],
    ecb(callback, finish)
  )
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
      if (semver.validRange(dependency.range) === null) {
        done(null, [
          {
            name: dependency.name,
            version: dependency.range,
            range: dependency.range,
            links: []
          }
        ])
      } else {
        // ...find the dependency tree for the highest version that
        // satisfies the range.
        self._maxSatisfying(
          sequence, dependency.name, dependency.range,
          function (error, result) {
            if (error) {
              /* istanbul ignore else */
              if (error.noSatisfying) {
                done(null, [
                  {
                    name: error.dependency.name,
                    range: error.dependency.range,
                    missing: true,
                    links: []
                  }
                ])
              } else {
                done(error)
              }
            } else {
              done(null, result)
            }
          }
        )
      }
    },

    // Once we have trees for dependencies...
    ecb(callback, function (dependencyTrees) {
      // ...combine them to form a new tree.
      var combinedTree = []
      dependencyTrees.forEach(function (tree) {
        mergeFlatTrees(combinedTree, tree)
      })
      sortFlatTree(combinedTree)
      callback(null, combinedTree)
    })
  )
}

var ZERO = packInteger(0)

// Find the tree for the highest package version that satisfies a given
// SemVer range.
prototype._maxSatisfying = function (sequence, name, range, callback) {
  var maxSatisfying = null
  pump(
    this._createTreeStream(sequence, name),
    to.obj(function (record, _, done) {
      var higherSatisfying = (
        semver.satisfies(record.version, range) &&
        (
          maxSatisfying === null ||
          semver.compare(maxSatisfying.version, record.version) === -1
        )
      )
      if (higherSatisfying) {
        maxSatisfying = record
      }
      done()
    })
  )
  .once('finish', function () {
    // If there isn't a match, yield an informative error with
    // structured data about the failed query.
    if (maxSatisfying === null) {
      callback({
        noSatisfying: true,
        dependency: {
          name: name,
          range: range
        }
      })
    // Have a tree for a package version that satisfied the range.
    } else {
      // Create a new tree with just a record for the top-level package.
      // The new records links to all direct dependencies in the tree.
      var treeWithDependency = [
        {
          name: name,
          version: maxSatisfying.version,
          range: range,
          // Link to all direct dependencies.
          links: maxSatisfying.tree
          .reduce(function (links, dependency) {
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
      maxSatisfying.tree.forEach(function (dependency) {
        delete dependency.range
      })

      mergeFlatTrees(maxSatisfying.tree, treeWithDependency)
      sortFlatTree(maxSatisfying.tree)
      callback(null, maxSatisfying.tree)
    }
  })
}

// Find all stored trees for a package at or before a given sequence.
prototype._createTreeStream = function (sequence, name) {
  return pump(
    this._levelup.createReadStream({
      gt: encodeKey(TREE_PREFIX, name, ZERO, ''),
      lt: encodeKey(TREE_PREFIX, name, sequence, '~'),
      reverse: true
    }),
    through.obj(function (data, _, done) {
      var decoded = decodeKey(data.key)
      done(null, {
        version: decoded[3],
        sequence: unpackInteger(decoded[2]),
        tree: data.value
      })
    })
  )
}

// Use key-only index records to find all direct and indirect dependents
// on a specific version of a specific package at or before a given
// sequence number.
prototype._createDependentsStream = function (sequence, name, version) {
  return pump(
    this._levelup.createReadStream({
      // Encode the low LevelUP key with an empty string suffix so
      // `encodeKey` will append the component separator, a slash.
      gt: encodeKey(DEPENDENCY_PREFIX, name, ZERO, ''),
      // LevelUP key components are URI-encoded ASCII, so the tilde
      // character is high.
      lt: encodeKey(DEPENDENCY_PREFIX, name, sequence, '~'),
      keys: true,
      // There are no meaningful values, so we can skip them.
      values: false
    }),
    through.obj(function (key, _, done) {
      var decoded = decodeKey(key)
      var range = decoded[3]
      if (semver.satisfies(version, range)) {
        done(null, {
          sequence: decoded[2],
          dependency: {
            name: decoded[1],
            range: range
          },
          dependent: {
            name: decoded[4],
            version: decoded[5]
          }
        })
      } else {
        done()
      }
    })
  )
}

prototype._getLastUpdate = function (name, callback) {
  var updateKey = encodeKey(UPDATE_PREFIX, name)
  this._levelup.get(updateKey, function (error, result) {
    if (error) {
      /* istanbul ignore else */
      if (error.notFound) {
        callback(null, [])
      } else {
        callback(error)
      }
    } else {
      callback(null, result)
    }
  })
}

prototype._putUpdate = function (chunk, callback) {
  var value = Object.keys(chunk.versions)
  .map(function (version) {
    return {
      updatedVersion: version,
      ranges: chunk.versions[version].dependencies
    }
  }, [])
  var updateKey = encodeKey(UPDATE_PREFIX, chunk.name)
  this._levelup.put(updateKey, value, callback)
}

prototype._updateVersion = function (sequence, version, callback) {
  var updatedName = version.updatedName
  var updatedVersion = version.updatedVersion
  var ranges = version.ranges
  var self = this
  var packed = packInteger(sequence)

  // Compute the flat package dependency manifest for the new package.
  self._treeFor(
    packed, updatedName, updatedVersion, ranges,
    ecb(callback, function (tree) {
      var missingDependencies = tree.filter(function (dependency) {
        return dependency.hasOwnProperty('missing')
      })
      var hasMissingDependencies = missingDependencies.length !== 0

      // We are missing some dependencies for this package.
      if (hasMissingDependencies) {
        missingDependencies.forEach(function (dependency) {
          self.emit('missing', {
            message: (
              'no package satisfying ' +
              dependency.name + '@' + dependency.range + ' for ' +
              updatedName + '@' + updatedVersion
            ),
            sequence: sequence,
            dependent: {
              name: updatedName,
              version: updatedVersion
            },
            dependency: {
              name: dependency.name,
              range: dependency.range
            }
          })
        })
      }

      var updatedBatch = []

      // Store the tree.
      pushTreeRecords(
        updatedBatch, updatedName, updatedVersion, tree, packed
      )

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
          updatedBatch.push({
            key: encodeKey(
              DEPENDENCY_PREFIX,
              dependencyName,
              packed,
              range,
              updatedName,
              updatedVersion
            )
          })
        })
      })

      completeBatch(updatedBatch)

      self._levelup.batch(
        updatedBatch,
        ecb(callback, function () {
          updatedBatch = null
          // Update trees for packages that directly and indirectly
          // depend on the updated package.
          pump(
            self._createDependentsStream(
              packed, updatedName, updatedVersion
            ),
            to.obj(function (dependent, _, done) {
              self._updateDependent(
                packed, updatedName, updatedVersion, tree,
                dependent, done
              )
            }),
            callback
          )
        })
      )
    })
  )
}

prototype._updateDependent = function (
  packed, updatedName, updatedVersion, tree, record, callback
) {
  var dependent = record.dependent
  var name = dependent.name
  var version = dependent.version
  var self = this

  // Find the most current tree for the package.
  self.query(
    name, version, packed,
    ecb(callback, function (result) {
      // Create a tree with:
      //
      // 1. the update package
      // 2. the updated package's dependencies
      //
      // and use it to update the existing tree for the
      // dependent package.
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

      treeClone.forEach(function (dependency) {
        // Demote direct dependencies to indirect dependencies.
        delete dependency.range
      })

      updateFlatTree(
        result,
        updatedName,
        updatedVersion,
        treeClone
      )
      sortFlatTree(result)

      var batch = []
      pushTreeRecords(
        batch, name, version, result, packed
      )
      completeBatch(batch)
      self._levelup.batch(batch, ecb(callback, function () {
        batch = null
        self.emit('updated', {
          dependency: {
            name: updatedName,
            version: updatedVersion
          },
          dependent: dependent
        })
        callback()
      }))
    })
  )
}

// Public API

// Get the flat dependency graph for a package and version at a specific
// sequence number.
prototype.query = function (name, version, sequence, callback) {
  var self = this
  if (typeof sequence === 'number') {
    sequence = packInteger(sequence)
  }
  var readStream = self._levelup.createReadStream({
    gt: encodeKey(POINTER_PREFIX, name, version, ''),
    lte: encodeKey(POINTER_PREFIX, name, version, sequence),
    reverse: true,
    limit: 1,
    keys: true,
    values: false
  })
  .once('error', /* istanbul ignore next */ function (error) {
    callback(error)
  })
  .on('data', function (key) {
    var decoded = decodeKey(key)
    readStream.destroy()
    var at = decoded[3]
    var resolvedKey = encodeKey(TREE_PREFIX, name, at, version)
    self._levelup.get(resolvedKey, ecb(callback, function (tree) {
      callback(null, tree, unpackInteger(at))
    }))
  })
  .once('end', function () {
    callback(null, null, null)
  })
}

// Get all currently know versions of a package, by name.
prototype.versions = function (name, callback) {
  var self = this
  var key = encodeKey(UPDATE_PREFIX, name)
  self._levelup.get(key, function (error, data) {
    if (error) {
      /* istanbul ignore else */
      if (error.notFound) {
        callback(null, null)
      } else {
        callback(error)
      }
    } else {
      var versions = data.map(function (element) {
        return element.updatedVersion
      })
      callback(null, versions)
    }
  })
}

// Get all currently known package names.
prototype.packages = function (name) {
  return pump(
    this._levelup.createReadStream({
      gt: encodeKey(UPDATE_PREFIX, ''),
      lte: encodeKey(UPDATE_PREFIX, '~'),
      keys: true,
      values: false
    }),
    through.obj(function (key, _, done) {
      var decoded = decodeKey(key)
      done(null, decoded[1])
    })
  )
}

// Get the last-processed sequence number.
prototype.sequence = function () {
  return this._sequence
}

// LevelUP String Encoding Helper Functions

var slice = Array.prototype.slice

function encodeKey (/* variadic */) {
  return slice.call(arguments)
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

function validName (argument) {
  return typeof argument === 'string' && argument.length !== 0
}

function validVersions (argument) {
  return typeof argument === 'object'
}

function completeBatch (batch) {
  batch.forEach(function (operation) {
    // Make operations put operations by default.
    operation.type = 'put'
    // Set a placeholder for key-only records.
    // These are used for indexing.
    if (!operation.hasOwnProperty('value')) {
      operation.value = ''
    }
  })
}

function pushTreeRecords (batch, name, version, tree, packed) {
  batch.push({
    key: encodeKey(TREE_PREFIX, name, packed, version),
    value: tree
  })
  batch.push({
    key: encodeKey(POINTER_PREFIX, name, version, packed)
  })
}

function changedVersions (oldUpdate, newUpdate) {
  // Turn the {$version: $object} map into an array.
  return Object.keys(newUpdate.versions)
  .map(function propertyToArrayElement (updatedVersion) {
    return {
      updatedVersion: updatedVersion,
      ranges: newUpdate.versions[updatedVersion].dependencies || {},
      updatedName: newUpdate.name
    }
  })
  // Filter out versions that haven't changed since the last
  // update for this package.
  .filter(function sameAsLastUpdate (newUpdate) {
    return !oldUpdate.some(function (priorUpdate) {
      return (
        priorUpdate.updatedVersion === newUpdate.updatedVersion &&
        deepEqual(priorUpdate.ranges, newUpdate.ranges)
      )
    })
  })
}
