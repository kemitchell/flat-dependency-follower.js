var Writable = require('stream').Writable
var asyncEach = require('async.each')
var asyncEachSeries = require('async-each-series')
var asyncMap = require('async.map')
var deepEqual = require('deep-equal')
var ecb = require('ecb')
var from2 = require('from2')
var fs = require('fs')
var inherits = require('util').inherits
var mergeFlatTrees = require('merge-flat-package-trees')
var mkdirp = require('mkdirp')
var normalize = require('normalize-registry-metadata')
var parseJSON = require('json-parse-errback')
var path = require('path')
var pump = require('pump')
var runWaterfall = require('run-waterfall')
var semver = require('semver')
var sortFlatTree = require('sort-flat-package-tree')
var split2 = require('split2')
var through2 = require('through2')
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

var UPDATE_PREFIX = 'last_updates'
var TREE_PREFIX = 'trees'
var DEPENDENCY_PREFIX = 'dependencies'

function FlatDependencyFollower (directory) {
  if (!(this instanceof FlatDependencyFollower)) {
    return new FlatDependencyFollower(directory)
  }
  this._directory = directory
  this._sequence = 0
  Writable.call(this, {
    objectMode: true,
    // Some of the documents in the registry are over 5MB.  Default,
    // 16-object stream buffers can fill available memory quickly, so
    // reduce to 2.
    highWaterMark: 2
  })
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
  self.emit('updating', updatedName)

  // Delete properties we don't need in memory.
  prune(chunk, ['name', 'versions'])
  var versions = chunk.versions
  Object.keys(versions).forEach(function (key) {
    prune(versions[key], ['dependencies'])
  })

  // Skip enormous updates.
  var versionCount = Object.keys(versions).length
  if (versionCount > 200) {
    self.emit('skipped', {
      name: updatedName,
      sequence: sequence,
      versions: versionCount
    })
    return callback()
  }

  function finish () {
    self._sequence = sequence
    self.emit('sequence', sequence)
    fs.writeFile(
      self._path(['sequence']),
      sequence.toString(),
      callback
    )
  }

  runWaterfall(
    [
      // Read the last saved update, which we will compare with the
      // current update to identify changed versions.
      function (done) {
        self._getLastUpdate(updatedName, done)
      },

      // Identify new and changed versions and process them.
      function (lastUpdate, done) {
        var versions = changedVersions(lastUpdate, chunk)
        lastUpdate = null
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
        range: (typeof ranges[dependencyName] === 'string')
          ? ranges[dependencyName]
          : 'INVALID'
      }
    }),

    // For each name-and-range pair...
    function (dependency, done) {
      var range = semver.validRange(dependency.range)
      if (range === null) {
        done(null, [
          {
            name: dependency.name,
            version: dependency.range,
            range: range,
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
    }),
    function () {
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
        // Create a new tree with just the top-level package.
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
    }
  )
}

// Find all stored trees for a package at or before a given sequence.
prototype._createTreeStream = function (sequence, name) {
  return filteredNDJSONStream(
    this._path([TREE_PREFIX, name]),
    function (chunk) {
      return chunk.sequence <= sequence
    }
  )
}

prototype._path = function (components) {
  return path.join.apply(path, [this._directory].concat(components))
}

// Use key-only index records to find all direct and indirect dependents
// on a specific version of a specific package at or before a given
// sequence number.
prototype._createDependentsStream = function (sequence, name, version) {
  return filteredNDJSONStream(
    this._path([DEPENDENCY_PREFIX, name]),
    function (chunk) {
      return (
        semver.satisfies(version, chunk.range) &&
        chunk.sequence <= sequence
      )
    }
  )
}

function filteredNDJSONStream (path, predicate) {
  var returned = through2.obj(function (chunk, _, done) {
    if (predicate(chunk)) {
      this.push(chunk)
    }
    done()
  })
  var source = fs
    .createReadStream(path)
    .once('error', function (error) {
      /* istanbul ignore else */
      if (error.code === 'ENOENT') {
        // Makes the returned stream an empty stream.
        returned.end()
      } else {
        // Pass the error through.
        returned.emit(error)
      }
    })
  return pump(source, parser(), returned)
}

prototype._getLastUpdate = function (name, callback) {
  var path = this._path([UPDATE_PREFIX, name])
  fs.readFile(path, function (error, buffer) {
    if (error) {
      /* istanbul ignore else */
      if (error.code === 'ENOENT') {
        callback(null, [])
      } else {
        callback(error)
      }
    } else {
      parseJSON(buffer, ecb(callback, function (object) {
        callback(null, object)
      }))
    }
  })
}

prototype._putUpdate = function (chunk, callback) {
  var value = Object.keys(chunk.versions).map(function (version) {
    return {
      updatedVersion: version,
      ranges: chunk.versions[version].dependencies
    }
  })
  var file = this._path([UPDATE_PREFIX, chunk.name])
  mkdirp(path.dirname(file), ecb(callback, function () {
    fs.writeFile(file, JSON.stringify(value), function (error) {
      callback(error)
    })
  }))
}

prototype._updateVersion = function (sequence, version, callback) {
  var updatedName = version.updatedName
  var updatedVersion = version.updatedVersion
  var ranges = version.ranges
  var self = this

  // Skip packages with too many dependencies.
  var dependencyCount = Object.keys(ranges).length
  if (dependencyCount > 45) {
    self.emit('skipped', {
      name: updatedName,
      sequence: sequence,
      dependencies: dependencyCount
    })
    return callback()
  }

  // Compute the flat package dependency manifest for the new package.
  self._treeFor(
    sequence, updatedName, updatedVersion, ranges,
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
        updatedBatch, updatedName, updatedVersion, tree, sequence
      )

      // Store key-only index records.  These will be used to
      // determine that this package's tree needs to be updated when
      // new versions of any of its dependencies---direct or
      // indirect---come in later.
      tree.forEach(function (dependency) {
        var dependencyName = dependency.name
        var withRanges = []

        // Direct dependency range.
        if (dependencyName.length !== 0 && dependencyName in ranges) {
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
          range = semver.validRange(range)
          if (range !== null) {
            updatedBatch.push({
              path: [
                DEPENDENCY_PREFIX,
                encodeURIComponent(dependencyName)
              ],
              value: {
                sequence: sequence,
                range: range,
                dependent: {
                  name: updatedName,
                  version: updatedVersion
                }
              }
            })
          }
        })
      })

      self._batch(
        updatedBatch,
        ecb(callback, function () {
          updatedBatch = null
          // Update trees for packages that directly and indirectly
          // depend on the updated package.
          pump(
            self._createDependentsStream(
              sequence, updatedName, updatedVersion
            ),
            to.obj(function (dependent, _, done) {
              self._updateDependent(
                sequence, updatedName, updatedVersion, tree,
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

prototype._batch = function (batch, callback) {
  var self = this
  asyncEachSeries(batch, function (instruction, done) {
    var file = self._path(instruction.path)
    mkdirp(path.dirname(file), ecb(done, function () {
      var value = JSON.stringify(instruction.value)
      fs.appendFile(file, value + '\n', done)
    }))
  }, callback)
}

prototype._updateDependent = function (
  sequence, updatedName, updatedVersion, tree, record, callback
) {
  var dependent = record.dependent
  var name = dependent.name
  var version = dependent.version
  var self = this

  // Find the most current tree for the package.
  self.query(
    name, version, sequence,
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
        links: treeClone.reduce(function (links, dependency) {
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
        batch, name, version, result, sequence
      )
      self._batch(batch, ecb(callback, function () {
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
  var latest = null
  pump(
    this._createTreeStream(sequence, name),
    to.obj(function (chunk, _, done) {
      if (
        chunk.version === version &&
        (latest === null || chunk.sequence > latest.sequence)
      ) {
        latest = chunk
      }
      done()
    }),
    function (error) {
      /* istanbul ignore if */
      if (error) {
        callback(error)
      } else {
        if (latest === null) {
          callback(null, null, null)
        } else {
          callback(null, latest.tree, latest.sequence)
        }
      }
    }
  )
}

// Get all currently know versions of a package, by name.
prototype.versions = function (name, callback) {
  var path = this._path([UPDATE_PREFIX, name])
  fs.readFile(path, function (error, buffer) {
    if (error) {
      /* istanbul ignore else */
      if (error.code === 'ENOENT') {
        callback(null, null)
      } else {
        callback(error)
      }
    } else {
      parseJSON(buffer, ecb(callback, function (record) {
        var versions = record.map(function (element) {
          return element.updatedVersion
        })
        callback(null, versions)
      }))
    }
  })
}

// Get all currently known package names.
prototype.packages = function (name) {
  var files = null
  var directory = this._path([UPDATE_PREFIX])
  return from2.obj(function source (_, next) {
    if (files === null) {
      fs.readdir(directory, ecb(next, function (read) {
        files = read
        source(_, next)
      }))
    } else {
      var file = files.shift()
      if (file) {
        next(null, decodeURIComponent(path.parse(file).name))
      } else {
        next(null, null)
      }
    }
  })
}

// Get the last-processed sequence number.
prototype.sequence = function () {
  return this._sequence
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

function pushTreeRecords (batch, name, version, tree, sequence) {
  batch.push({
    path: [TREE_PREFIX, encodeURIComponent(name)],
    value: {
      version: version,
      sequence: sequence,
      tree: tree
    }
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

function prune (object, keysToKeep) {
  var keys = Object.keys(object)
  var length = keys.length
  for (var index = 0; index < length; index++) {
    var key = keys[index]
    /* istanbul ignore if */
    if (keysToKeep.indexOf(key) === -1) {
      delete object[key]
    }
  }
}

function parser () {
  return split2(function (line) {
    try {
      if (line) {
        return JSON.parse(line)
      }
    } catch (error) {
      // Pass.
    }
  }, {highWaterMark: 4})
}
