var Writable = require('stream').Writable
var asyncEach = require('async.each')
var asyncEachSeries = require('async-each-series')
var asyncMap = require('async.map')
var deepEqual = require('deep-equal')
var ecb = require('ecb')
var from2 = require('from2')
var fs = require('fs')
var inherits = require('util').inherits
var lexint = require('lexicographic-integer')
var mergeFlatTrees = require('merge-flat-package-trees')
var mkdirp = require('mkdirp')
var normalize = require('normalize-registry-metadata')
var parseJSON = require('json-parse-errback')
var path = require('path')
var pump = require('pump')
var recursiveReaddir = require('recursive-readdir')
var runWaterfall = require('run-waterfall')
var semver = require('semver')
var sortFlatTree = require('sort-flat-package-tree')
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

// Directory Structure
//
// The follower stores data in a directory on the file system.
//
// Last Updates
//
//   last_updates/{name} -> [{version, dependencies}, ...]
//
// Dependency Trees
//
//   trees/{name}/{sequence}/{version} -> Array
//
// These records store the precomputed flat package trees.  The prefix
// leads with sequence, rather than version, because Semantic Versions
// strings aren't lexicographically ordered.
//
//   links/{name}/{version}/{sequence}
//
// `prototype.query` uses these "pointer" links to find the last tree
// record key for a package by sequence number.
//
// Dependency Relationships
//
//   dependencies/{dependency}/{sequence}/{range}/{dependent}/{version}
//
// `prototype._findDependents` uses these paths to identify existing
// package trees that need to be updated.
var UPDATE_PREFIX = 'last_updates'
var TREE_PREFIX = 'trees'
var LINK_PREFIX = 'links'
var DEPENDENCY_PREFIX = 'dependencies'

function FlatDependencyFollower (directory) {
  if (!(this instanceof FlatDependencyFollower)) {
    return new FlatDependencyFollower(directory)
  }
  this._directory = directory
  this._sequence = 0
  Writable.call(this, {
    objectMode: true,
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
  var self = this
  var directory = self._path(TREE_PREFIX, name)
  var matches = null
  return from2.obj(function source (_, next) {
    if (matches === null) {
      recursiveReaddir(directory, function (error, read) {
        if (error) {
          next(null, null)
        } else {
          matches = read
            .map(function (file) {
              var split = file.split('/')
              var length = split.length
              return {
                file: file,
                version: split[length - 1],
                sequence: unpackInteger(split[length - 2])
              }
            })
            .filter(function (record) {
              return record.sequence <= sequence
            })
            .sort(function (a, b) {
              /* istanbul ignore else */
              if (a.version < b.version) {
                return -1
              } else if (a.version > b.version) {
                return 1
              } else {
                return 0
              }
            })
            .reverse()
          source(_, next)
        }
      })
    } else {
      var match = matches.shift()
      if (match) {
        fs.readFile(match.file, ecb(next, function (read) {
          parseJSON(read, ecb(next, function (object) {
            next(null, {
              version: match.version,
              sequence: match.sequence,
              tree: object
            })
          }))
        }))
      } else {
        next(null, null)
      }
    }
  })
}

prototype._path = function (/* variadic */) {
  var args = Array.prototype.slice.call(arguments)
  return path.join.apply(path, [this._directory].concat(args))
}

// Use key-only index records to find all direct and indirect dependents
// on a specific version of a specific package at or before a given
// sequence number.
prototype._createDependentsStream = function (sequence, name, version) {
  var directory = this._path(DEPENDENCY_PREFIX, name)
  var dependents = null
  return from2.obj(function source (_, next) {
    if (dependents === null) {
      recursiveReaddir(directory, function (error, read) {
        if (error) {
          /* istanbul ignore else */
          if (error.code === 'ENOENT') {
            next(null, null)
          } else {
            next(error)
          }
        } else {
          dependents = read
            .map(function (file) {
              var split = file.split('/')
              var length = split.length
              var record = {
                sequence: unpackInteger(split[length - 4]),
                dependency: {
                  name: split[length - 5],
                  range: split[length - 3]
                },
                dependent: {
                  name: split[length - 2],
                  version: split[length - 1]
                }
              }
              return record
            })
            .filter(function (record) {
              return (
                record.sequence <= sequence &&
                semver.satisfies(version, record.dependency.range)
              )
            })
          source(_, next)
        }
      })
    } else {
      var dependent = dependents.shift()
      if (dependent) {
        next(null, dependent)
      } else {
        next(null, null)
      }
    }
  })
}

prototype._getLastUpdate = function (name, callback) {
  var path = this._path(UPDATE_PREFIX, name)
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
  var file = this._path(UPDATE_PREFIX, chunk.name)
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
            path: path.join(
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

      self._batch(
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

prototype._batch = function (batch, callback) {
  var self = this
  asyncEachSeries(batch, function (instruction, done) {
    var file = self._path(instruction.path)
    var directory = path.dirname(file)
    mkdirp(directory, ecb(done, function () {
      if (instruction.link) {
        var target = self._path(instruction.link)
        fs.symlink(target, file, done)
      } else {
        fs.writeFile(file, JSON.stringify(instruction.value), done)
      }
    }))
  }, callback)
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
  var self = this
  if (typeof sequence === 'number') {
    sequence = packInteger(sequence)
  }
  var directory = self._path(LINK_PREFIX, name, version)
  fs.readdir(directory, function (error, read) {
    /* istanbul ignore if */
    if (error) {
      if (error.code === 'ENOENT') {
        callback(null, null, null)
      } else {
        callback(error)
      }
    } else {
      var links = read.sort().reverse()
      var length = links.length
      for (var index = 0; index < length; index++) {
        var link = links[index]
        if (link > sequence) {
          continue
        } else {
          var linkPath = path.join(directory, link)
          return fs.readlink(linkPath, function (error, file) {
            /* istanbul ignore if */
            if (error) {
              if (error.code === 'ENOENT') {
                callback(null, null, null)
              } else {
                callback(error)
              }
            } else {
              fs.readFile(file, ecb(callback, function (buffer) {
                parseJSON(buffer, ecb(callback, function (record) {
                  callback(null, record, unpackInteger(link))
                }))
              }))
            }
          })
        }
      }
      callback(null, null, null)
    }
  })
}

// Get all currently know versions of a package, by name.
prototype.versions = function (name, callback) {
  var path = this._path(UPDATE_PREFIX, name)
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
  var directory = this._path(UPDATE_PREFIX)
  return from2.obj(function source (_, next) {
    if (files === null) {
      fs.readdir(directory, ecb(next, function (read) {
        files = read
        source(_, next)
      }))
    } else {
      var file = files.shift()
      if (file) {
        next(null, path.parse(file).name)
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

// LevelUP String Encoding Helper Functions

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
    path: path.join(TREE_PREFIX, name, packed, version),
    value: tree
  })
  batch.push({
    path: path.join(LINK_PREFIX, name, version, packed),
    link: path.join(TREE_PREFIX, name, packed, version)
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
