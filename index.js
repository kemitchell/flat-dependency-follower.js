// Notes for Readers
//
// This code brings a lot of work together in a useful way, but with the
// exception of some neat streaming flat-file stuff, for performance,
// all the good ideas are implemented elsewhere.  In particular, you
// should definitely become fluent in these before diving in:
//
// 1.  pull-streams:  leaner, meaner, cleaner streams, implemented with
//     plain JavaScript functions.  See the README for pull-stream.
//     This was originally implemented with Node streams, but that
//     became a mighty pain.  Yes, Node already has a very ingrained
//     streams implementation, but almost everyone finds a dive into
//     pull-stream rewarding.
//
// 2.  node-semver:  If you've been looking for an excuse to look into
//     how node-semver satisfies dependency ranges, desugars caret
//     syntax, and so on, the time is now!  The resolution used here
//     is a bit more "pure" than npm CLI when multiple dependencies
//     are involved.
//
// 3.  flat package trees:  The follower uses a specific schema to
//     describe dependencies, how they are resolved, and what they in
//     turn depend on.  See the README examples for this package and
//     update-flat-package-tree.  Most of the original, useful thinking
//     here is in the flat-package-tree packages.
//
// 4.  replicate.npmjs.com:  An endpoint for replicating the npm public
//     registry, using the CouchDB HTTP replication API.  Good folk at
//     npm have published a tutorial on writing "registry followers"
//     using the API, but it uses lots of Node stream-based helper
//     packages that I abandoned for various reasons.  Better to have
//     a quick look at a response from replicate.npmjs.com, and
//     possibly the CouchDB API doc.

// Flat Dependency Trees
var mergeFlatTrees = require('merge-flat-package-trees')
var sortFlatTree = require('sort-flat-package-tree')
var updateFlatTree = require('update-flat-package-tree')
var semver = require('semver')

// Streams
var decodeUTF8 = require('pull-utf8-decoder')
var defer = require('pull-defer')
var filter = require('pull-stream').filter
var map = require('pull-stream').map
var pull = require('pull-stream')
var readFile = require('pull-file')
var reduce = require('pull-stream').reduce
var split = require('pull-split')

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
exports.maxSatisfying = findMaxSatisfying

// Overview of Flat-File Storage Scheme
//
// The follower stores all its state in flat files within a single
// provided directory.
//
// Whenever a package name appears in a file name, the package name is
// encoded with `encodeURIComponent`.
//
// There are four kinds of data files:
//
// 1. ./sequence
//
//     Format: JSON Number
//
//     Content: last successfully processed update sequence number
//
//     The sequence file is overwritten after all other processing for a
//     given CouchDB update.
//
// 2. ./$UPDATE/$name
//
//     Format: Newline-delimited JSON
//
//     Content: {sequence, versions: [{updatedVersion, ranges}]}
//
//     Storing these enables the follower to compare each new update for
//     a package with the last update.  The follower need process only
//     versions that were changed.
//
// 3. ./$TREE/$name
//
//     Format: Newline-delimited JSON
//
//     Content: {sequence, version, tree}
//
//     Fully resolved flat dependency trees for every known version of
//     the package, as of the most recent update.  The follower
//     replaces the lines for each version when dependencies are
//     updated.
//
//     The follower does _not_ sort objects in the file.
//
// 4. ./$DEPENDENCY/$dependency-name
//
//      Format: Newline-delimited JSON
//
//      Content: {sequence, range, dependent: {name, version}}
//
//      One line for each dependent package version.
//
//      The follower does _not_ sort objects in the file.
//
// Prior approaches had a number of shortcomings:
//
// 1.  They exceeded the filesystem inode limit.
//
//     With more than a quarter-million distinct packages in the public
//     registry and growing, two files per package are doable, but a
//     file for each version of each package is not.
//
// 2.  Too slow to keep up with the registry.
//
//     Using streaming formats throughout gives the follower a fighting
//     chance on a solid-state disk drive.
//
// 3.  They consumed too much memory.
//
//     Some packages have a staggering number of dependents, or a huge
//     number of versions, or both.  Any file with more than one record
//     can potentially exceed the size of available memory.  Again,
//     stream processing to the rescue.

// Data file and directory name constants.
var DEPENDENCY = 'dependencies'
var SEQUENCE = 'sequence'
var TREE = 'trees'
var UPDATE = 'updates'

// The tree property value in tree records for unpublished packages.
var UNPUBLISHED_TOMBSTONE = 'unpublished'

// A pull-stream sink for consuming update objects from the npm public
// registry's CouchDB-style replication endpoint.
function sink (directory, log, callback) {
  return pull(
    filter(isPackageUpdate),
    map(removeUnnecessaryProperties),
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

// The sink needs just a few of the properties of the update objects
// that the replication endpoint provides.  The unneeded properties can
// actually take up quite a bit of memory.  In particular, the full
// update documents include the README text of every version of the
// changed package.
function removeUnnecessaryProperties (update) {
  delete update.changes
  var doc = update.doc
  prune(doc, ['versions', 'name'])
  var versions = doc.versions
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

    // Identify new, changed, and unpublished versions and process them.
    function (last, done) {
      var changes = findChangedVersions(last, update)
      changes.lastUpdateSequence = last.sequence
      changes.publishedVersionStrings = changes
        .published
        .map(function (element) {
          return element.updatedVersion
        })
        .filter(function (version) {
          return semver.valid(version) !== null
        })
      log.info({
        published: changes.publishedVersionStrings,
        unpublished: changes.unpublishedVersionStrings
      }, 'changed')
      done(null, changes)
    },

    // TODO: Update files to reflect unpublishes.
    function (changes, done) {
      // changes.unpublishedVersionStrings
      done(null, changes)
    },

    function (changes, done) {
      eachSeries(changes.published, function (version, done) {
        writeVersion(
          directory, log, update.sequence,
          version, changes.publishedVersionStrings,
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
  directory, sequence, name, range, callback
) {
  pull(
    treesStream(directory, sequence, name),
    reduce(matchesFor(range), [], ecb(callback, function (matches) {
      // If there isn't a match, yield an informative error with
      // structured data about the failed query.
      if (matches.length === 0) {
        callback({
          noSatisfying: true,
          dependency: {
            name: name,
            range: range
          }
        })
      // Have a tree for a package version that satisfied the range.
      } else {
        var max = matches[matches.length - 1]
        // Create a new tree with just the top-level package. The new
        // record links to all direct dependencies in the tree.
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

function matchesFor (range) {
  // This function is backwards---accumulator first, then new
  // data---because that's what pull-stream's reduce expects.
  return function (matches, candidate) {
    var version = candidate.version
    function sameVersion (priorMatch) {
      return priorMatch.version === version
    }
    try {
      if (semver.satisfies(version, range)) {
        // If we matched the same version at a prior sequence number,
        // remove the version at the old sequence number.
        var alreadyMatchedVersion = matches.some(sameVersion)
        if (alreadyMatchedVersion) {
          matches = matches.filter(sameVersion)
        }
        // Unless the package was unpublished, add this new record to
        // the results list and sort it.
        if (candidate.tree !== UNPUBLISHED_TOMBSTONE) {
          matches.push(candidate)
          matches.sort(sortRecords)
        }
      }
      return matches
    } catch (error) {
      return matches
    }
  }
  function sortRecords (a, b) {
    return semver.compare(a.version, b.version)
  }
}

// Find all stored trees for a package at or before a given sequence.
function treesStream (directory, sequence, name) {
  return filteredNDJSONStream(
    join(directory, TREE, encode(name)),
    function (chunk) {
      return chunk.sequence <= sequence
    }
  )
}

// Use key-only index records to find all direct and indirect dependents
// on a specific version of a specific package at or before a given
// sequence number.
function dependentsStream (directory, sequence, name, version) {
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
            semver.satisfies(version, chunk.range) &&
            chunk.sequence <= sequence
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

// Swallow ENOENT errors from a pipeline, ending the stream instead of
// yielding the error.  Used where the nonexistence of a requested file
// indicates just that the stream of data it represents is empty.
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

// Read the last update, if any, for the given package.  The last update
// for each seen package is cached, so the sink can compare new updates
// and process only those versions that have changed.
function readLastUpdate (directory, name, callback) {
  var path = join(directory, UPDATE, encode(name))
  fs.readFile(path, function (error, buffer) {
    if (error) {
      /* istanbul ignore else */
      if (error.code === 'ENOENT') {
        callback(null, {
          versions: [],
          sequence: -1
        })
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

// Save an update from the registry replication update to disk.
function saveUpdate (directory, chunk, callback) {
  var value = {
    sequence: chunk.sequence,
    versions: Object.keys(chunk.versions).map(function (version) {
      return {
        updatedVersion: version,
        ranges: chunk.versions[version].dependencies
      }
    })
  }
  var file = join(directory, UPDATE, encode(chunk.name))
  mkdirp(dirname(file), ecb(callback, function () {
    fs.writeFile(file, JSON.stringify(value), callback)
  }))
}

function writeVersion (
  directory, log, sequence, version, otherUpdatedVersions, callback
) {
  var updatedName = version.updatedName
  var updatedVersion = version.updatedVersion
  var ranges = version.ranges

  // Compute the flat package dependency manifest for the new package.
  calculateTreeFor(
    directory, sequence, updatedName, updatedVersion, ranges,
    ecb(callback, function (tree) {
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
              path: join(DEPENDENCY, encode(dependencyName)),
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

      writeBatch(directory, updatedBatch, ecb(callback, function () {
        log.info({
          name: updatedName,
          version: updatedVersion
        }, 'wrote tree')

        // Update trees for packages that directly and indirectly
        // depend on the updated package.
        pull(
          dependentsStream(
            directory, sequence, updatedName, updatedVersion
          ),
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
                    directory, sequence, updatedName, updatedVersion,
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
function calculateTreeFor (
  directory, sequence, name, version, ranges, callback
) {
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
          directory, sequence, dependency.name, dependency.range,
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
    // TODO check array arg
    var file = join(directory, instruction.path)

    // Create the directory for the file if it doesn't already exist.
    mkdirp(dirname(file), ecb(done, function () {
      var value = JSON.stringify(instruction.value)
      fs.appendFile(file, value + '\n', done)
    }))
  }, callback)
}

// Once we've updated the package tree for an updated package, go back
// and update the trees for all the packages that depend on it, directly
// or indirectly.
function updateDependent (
  directory, sequence, updatedName, updatedVersion,
  tree, record, callback
) {
  var dependent = record.dependent
  var name = dependent.name
  var version = dependent.version

  // Find the most current tree for the package.
  readTree(
    directory, name, version, sequence,
    ecb(callback, function (result) {
      // Create a tree with:
      //
      // 1. the updated package
      // 2. the updated package's dependencies
      //
      // and use it to update the existing tree for the
      // dependent package.
      var treeClone = deepClone(tree)

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
      pushTreeRecords(batch, name, version, result, sequence)
      writeBatch(directory, batch, callback)
    })
  )
}

// Get the flat dependency graph for a package and version at a specific
// sequence number.
function readTree (directory, name, version, sequence, callback) {
  pull(
    treesStream(directory, sequence, name),
    filter(function (update) {
      return update.version === version
    }),
    reduce(laterUpdate, null, ecb(callback, function (latest) {
      if (latest) {
        callback(null, latest.tree, latest.sequence)
      } else {
        callback(null, null, null)
      }
    }))
  )
}

function laterUpdate (a, b) {
  return (a === null || a.sequence < b.sequence) ? b : a
}

// Get all currently know versions of a package, by name.
function versions (directory, name, callback) {
  var path = join(directory, UPDATE, encode(name))
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
        var versions = record.versions.map(function (element) {
          return element.updatedVersion
        })
        callback(null, versions)
      }))
    }
  })
}

// Get all currently known package names.
function packages (directory, name) {
  directory = join(directory, UPDATE)
  var deferred = defer.source()
  fs.readdir(directory, function (error, read) {
    deferred.resolve(
      error
        ? pull.error(error)
        : pull(pull.values(read), map(decode))
    )
  })
  return deferred
}

// Get the last-processed sequence number.
function sequence (directory, callback) {
  var file = join(directory, SEQUENCE)
  fs.readFile(file, ecb(callback, function (buffer) {
    callback(null, parseInt(buffer.toString()))
  }))
}

// Helper Functions

function deepClone (argument) {
  return JSON.parse(JSON.stringify(argument))
}

function validName (argument) {
  return typeof argument === 'string' && argument.length !== 0
}

function pushTreeRecords (batch, name, version, tree, sequence) {
  batch.push({
    path: join(TREE, encode(name)),
    value: {
      version: version,
      sequence: sequence,
      tree: tree
    }
  })
}

function findChangedVersions (oldUpdate, newUpdate) {
  // Turn the {$version: $object} map into an array.
  var returned = {}
  returned.published = Object.keys(newUpdate.versions)
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
      return !oldUpdate.versions.some(function (priorUpdate) {
        return (
          priorUpdate.updatedVersion === newUpdate.updatedVersion &&
          deepEqual(priorUpdate.ranges, newUpdate.ranges)
        )
      })
    })
  returned.unpublishedVersionStrings = oldUpdate.versions
    .map(function (version) {
      return version.updatedVersion
    })
    .filter(function notInNewUpdate (version) {
      return !newUpdate.versions.hasOwnProperty(version)
    })
  return returned
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
