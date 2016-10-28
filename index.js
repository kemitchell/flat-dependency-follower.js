// Flat Dependency Trees
var mergeFlatTrees = require('merge-flat-package-trees')
var sortFlatTree = require('sort-flat-package-tree')
var updateFlatTree = require('update-flat-package-tree')
var semver = require('semver')

// Streams
var cat = require('pull-cat')
var decodeUTF8 = require('pull-utf8-decoder')
var filter = require('pull-stream').filter
var map = require('pull-stream').map
var pull = require('pull-stream')
var readFile = require('pull-file')
var reduce = require('pull-stream').reduce
var split = require('pull-split')
var writeFile = require('pull-write-file')

// Control Flow
var asyncMap = require('async.map')
var eachSeries = require('async-each-series')
var ecb = require('ecb')
var parseJSON = require('json-parse-errback')
var runWaterfall = require('run-waterfall')

// Miscellany
var deepEqual = require('deep-equal')
var dirname = require('path').dirname
var fs = require('fs')
var join = require('path').join
var mkdirp = require('mkdirp')
var parse = require('path').parse

var encode = encodeURIComponent
var decode = decodeURIComponent

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

exports.packages = packages
exports.tree = readTree
exports.sequence = sequence
exports.sink = sink
exports.versions = versions

var DEPENDENCY = 'dependencies'
var SEQUENCE = 'sequence'
var TREE = 'trees'
var UPDATE = 'updates'

function sink (directory, log, callback) {
  return pull(
    filter(isPackageUpdate),
    map(pruneUpdate),
    function (source) {
      source(null, next)
      function next (end, update) {
        if (end === true && callback) {
          callback()
        } else if (end && callback) {
          callback(end)
        } else {
          writeUpdate(directory, log, update, function (error) {
            if (error) {
              source(true, function () {
                callback(error)
              })
            } else {
              source(null, next)
            }
          })
        }
      }
    }
  )
}

function isPackageUpdate (update) {
  return (
    update.doc &&
    validName(update.doc.name) &&
    update.doc.hasOwnProperty('versions')
  )
}

function pruneUpdate (update) {
  delete update.changes
  var doc = update.doc
  prune(doc, ['versions', 'name'])
  var versions = doc.versions
  /* istanbul ignore else */
  if (versions) {
    Object.keys(versions).forEach(function (version) {
      prune(versions[version], 'dependencies')
    })
  }
  doc.sequence = update.seq
  return doc
}

function writeUpdate (directory, log, update, callback) {
  runWaterfall([
    // Read the last saved update, which we will compare with the
    // current update to identify changed versions.
    function (done) {
      readLastUpdate(directory, update.name, done)
    },

    // Identify new and changed versions and process them.
    function (last, done) {
      var changed = changedVersions(last, update)
      var versions = changed
        .map(function (element) {
          return element.updatedVersion
        })
        .filter(function (version) {
          return semver.valid(version) !== null
        })
      log.info({
        versions: versions
      }, 'changed')
      eachSeries(changed, function (version, done) {
        writeVersion(
          directory, log, version, versions,
          ecb(done, function () {
            log.info({
              name: version.updatedName,
              version: version.updatedVersion
            }, 'updated version')
            done()
          })
        )
      }, done)
    },

    // Overwrite the update record for this package, so we can compare
    // it to the next update for this package later.
    function (done) {
      saveUpdate(directory, update, ecb(done, function () {
        log.info({
          name: update.name
        }, 'saved update')
        done()
      }))
    },

    // Overwrite the sequence number file.
    function (done) {
      fs.writeFile(
        join(directory, SEQUENCE),
        update.sequence.toString() + '\n',
        done
      )
    }
  ], ecb(callback, function () {
    log.info({
      sequence: update.sequence,
      name: update.name
    }, 'finished update')
    callback()
  }))
}

// Find the tree for the highest package version that satisfies a given
// SemVer range.
function findMaxSatisfying (
  directory, name, range, callback
) {
  pull(
    treesStream(directory, name),
    reduce(higherMatch(range), null, ecb(callback, function (max) {
      // If there isn't a match, yield an informative error with
      // structured data about the failed query.
      if (max === null) {
        callback({
          noSatisfying: true,
          dependency: {
            name: name,
            range: range
          }
        })
      // Have a tree for a package version that satisfied the range.
      } else {
        // Create a new tree with just the top-level package. The new
        // records links to all direct dependencies in the tree.
        var treeWithDependency = [
          {
            name: name,
            version: max.version,
            range: range,
            // Link to all direct dependencies.
            links: max.tree.reduce(function (links, dependency) {
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
        max.tree.forEach(function (dependency) {
          delete dependency.range
        })

        mergeFlatTrees(max.tree, treeWithDependency)
        sortFlatTree(max.tree)
        callback(null, max.tree)
      }
    }))
  )
}

function higherMatch (range) {
  return function (a, b) {
    try {
      return (
        semver.satisfies(b.version, range) &&
        (a === null || semver.compare(a.version, b.version) === -1)
      ) ? b : a
    } catch (error) {
      return a
    }
  }
}

// Find stored trees for a package.
function treesStream (directory, name) {
  return filteredNDJSONStream(
    join(directory, TREE, encode(name)),
    function (chunk) {
      return true
    }
  )
}

// Use key-only index records to find all direct and indirect dependents
// on a specific version of a specific package.
function dependentsStream (directory, name, version) {
  var seen = {}
  return filteredNDJSONStream(
    join(directory, DEPENDENCY, encode(name)),
    function (chunk) {
      var dependent = chunk.dependent
      if (!seen.hasOwnProperty(dependent.name)) {
        seen[dependent.name] = []
      }
      var alreadySeen = seen[dependent.name]
        .indexOf(dependent.version) !== -1
      if (alreadySeen) {
        return false
      } else {
        seen[dependent.name].push(dependent.version)
        try {
          return (
            !alreadySeen &&
            semver.satisfies(version, chunk.range)
          )
        } catch (error) {
          return false
        }
      }
    }
  )
}

function filteredNDJSONStream (path, predicate) {
  return pull(
    readFile(path),
    decodeUTF8(),
    split(
      '\n',
      false, // Do not map.
      false, // Do not reverse.
      true // Skip the last.
    ),
    map(JSON.parse),
    filter(predicate),
    suppressENOENT
  )
}

function suppressENOENT (source) {
  return function sink (end, callback) {
    source(end, function (end, data) {
      if (end && end !== true && end.code === 'ENOENT') {
        end = true
      }
      callback(end, data)
    })
  }
}

function readLastUpdate (directory, name, callback) {
  var path = join(directory, UPDATE, encode(name))
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

function saveUpdate (directory, chunk, callback) {
  var value = Object.keys(chunk.versions).map(function (version) {
    return {
      updatedVersion: version,
      ranges: chunk.versions[version].dependencies
    }
  })
  var file = join(directory, UPDATE, encode(chunk.name))
  mkdirp(dirname(file), ecb(callback, function () {
    fs.writeFile(file, JSON.stringify(value), callback)
  }))
}

function writeVersion (
  directory, log, version, otherUpdatedVersions, callback
) {
  var updatedName = version.updatedName
  var updatedVersion = version.updatedVersion
  var ranges = version.ranges

  // Compute the flat package dependency manifest for the new package.
  calculateTreeFor(
    directory, updatedName, updatedVersion, ranges,
    ecb(callback, function (tree) {
      var updatedBatch = []

      // Store the tree.
      pushTreeRecords(updatedBatch, updatedName, updatedVersion, tree)

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
              type: 'append',
              path: join(DEPENDENCY, encode(dependencyName)),
              value: {
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

      writeBatch(directory, updatedBatch, ecb(callback, function () {
        log.info({
          name: updatedName,
          version: updatedVersion
        }, 'wrote tree')

        // Update trees for packages that directly and indirectly
        // depend on the updated package.
        pull(
          dependentsStream(directory, updatedName, updatedVersion),
          function sink (source) {
            source(null, function next (end, dependent) {
              if (end === true) {
                callback(end === true ? null : end)
              } else {
                var range = dependent.range
                var max = semver.maxSatisfying(
                  otherUpdatedVersions, range
                )
                // Some other version modified by this update better
                // matches the range.
                if (max !== updatedVersion) {
                  source(null, next)
                } else {
                  updateDependent(
                    directory, updatedName, updatedVersion,
                    tree, dependent,
                    ecb(callback, function () {
                      log.info({
                        dependent: dependent.dependent,
                        dependency: {
                          name: updatedName,
                          version: updatedVersion,
                          range: range
                        }
                      }, 'updated dependent')
                      source(null, next)
                    })
                  )
                }
              }
            })
          }
        )
      }))
    })
  )
}

// Generate a tree for a package, based on the `.dependencies` object in
// its `package.json`.
function calculateTreeFor (directory, name, version, ranges, callback) {
  asyncMap(
    // Turn the Object mapping from package name to SemVer range into an
    // Array of Objects with name and range properties.
    Object.keys(ranges)
      .map(function (dependencyName) {
        return {
          name: dependencyName,
          range: (typeof ranges[dependencyName] === 'string')
            ? ranges[dependencyName]
            : 'INVALID'
        }
      })
      .filter(function (dependency) {
        return dependency.name.length !== 0
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
        findMaxSatisfying(
          directory, dependency.name, dependency.range,
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

function writeBatch (directory, batch, callback) {
  eachSeries(batch, function (instruction, done) {
    var file = join(directory, instruction.path)
    mkdirp(dirname(file), ecb(done, function () {
      var value = JSON.stringify(instruction.value)
      /* istanbul ignore else */
      if (instruction.type === 'append') {
        fs.appendFile(file, value + '\n', done)
      } else if (instruction.type === 'replace') {
        fs.stat(file, function (error, stats) {
          var missing = error && error.code === 'ENOENT'
          if (error && !missing) {
            done(error)
          } else {
            var source = pull(
              cat([
                filteredNDJSONStream(file, function (chunk) {
                  return chunk.version !== instruction.value.version
                }),
                pull.once(instruction.value)
              ]),
              map(function makeBuffersForWriteFile (chunk) {
                return Buffer.from(JSON.stringify(chunk) + '\n')
              })
            )
            if (missing) {
              pull(source, writeFile(file, done))
            } else {
              var temporary = join(directory, 'temporary')
              pull(source, writeFile(temporary, ecb(done, function () {
                fs.rename(temporary, file, done)
              })))
            }
          }
        })
      } else {
        done(new Error('Invalid instruction type: ' + instruction.type))
      }
    }))
  }, callback)
}

function updateDependent (
  directory, updatedName, updatedVersion,
  tree, record, callback
) {
  var dependent = record.dependent
  var name = dependent.name
  var version = dependent.version

  // Find the most current tree for the package.
  readTree(
    directory, name, version,
    ecb(callback, function (result) {
      // Create a tree with:
      //
      // 1. the updated package
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

      // Demote direct dependencies to indirect dependencies.
      treeClone.forEach(function (dependency) {
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
      pushTreeRecords(batch, name, version, result)
      writeBatch(directory, batch, callback)
    })
  )
}

// Get the flat dependency graph for a package and version.
function readTree (directory, name, version, callback) {
  pull(
    treesStream(directory, name),
    pull.find(
      function (tree) {
        return tree.version === version
      },
      ecb(callback, function (record) {
        callback(null, record.tree)
      })
    )
  )
}

// Get all currently know versions of a package, by name.
function versions (directory, name, callback) {
  var file = join(directory, TREE, encode(name))
  pull(
    filteredNDJSONStream(file, function (chunk) {
      return true
    }),
    reduce(function (reduction, current) {
      return reduction.concat(current.version)
    }, [], callback)
  )
}

// Get all currently known package names.
function packages (directory, name) {
  directory = join(directory, UPDATE)
  var files
  return function source (end, callback) {
    if (files === undefined) {
      fs.readdir(directory, ecb(callback, function (read) {
        files = read
        source(end, callback)
      }))
    } else {
      var file = files.shift()
      if (file) {
        callback(null, decode(parse(file).name))
      } else {
        callback(true)
      }
    }
  }
}

// Get the last-processed sequence number.
function sequence (directory, callback) {
  var file = join(directory, SEQUENCE)
  fs.readFile(file, ecb(callback, function (buffer) {
    callback(null, parseInt(buffer.toString()))
  }))
}

// Helper Functions

function clone (argument) {
  return JSON.parse(JSON.stringify(argument))
}

function validName (argument) {
  return typeof argument === 'string' && argument.length !== 0
}

function pushTreeRecords (batch, name, version, tree) {
  batch.push({
    type: 'replace',
    path: join(TREE, encode(name)),
    value: {
      version: version,
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
