var Writable = require('stream').Writable
var asyncMap = require('async.map')
var find = require('array-find')
var inherits = require('util').inherits
var lexint = require('lexicographic-integer')
var merge = require('merge-flat-package-trees')
var semver = require('semver')
var update = require('update-flat-package-tree')

module.exports = FlatDependencyFollower

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
  var publishedName = chunk.name
  var sequence = chunk.sequence
  var batch = []
  function writeBatch () {
    batch.forEach(function (operation) {
      operation.type = 'put'
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
  Object.keys(chunk.versions).forEach(function (publishedVersion) {
    var ranges = chunk.versions[publishedVersion].dependencies

    batch.push({
      key: encode(
        'ranges',
        publishedName,
        publishedVersion,
        pack(sequence)
      ),
      value: ranges
    })

    Object.keys(ranges).forEach(function (dependencyName) {
      batch.push({
        key: encode(
          'dependent',
          dependencyName,
          pack(sequence),
          ranges[dependencyName],
          publishedName,
          publishedVersion
        )
      })
    })

    self._treeFor(
      sequence, publishedName, publishedVersion, ranges,
      function (error, tree) {
        /* istanbul ignore if */
        if (error) {
          callback(error)
        } else {
          batch.push({
            key: encode(
              'tree',
              publishedName,
              pack(sequence),
              publishedVersion
            ),
            value: tree
          })

          self._findDependents(
            sequence, publishedName, publishedVersion,
            function (error, dependents) {
              /* istanbul ignore if */
              if (error) {
                callback(error)
              } else {
                asyncMap(
                  dependents,
                  function (record, done) {
                    var dependent = record.dependent
                    self._findTree(
                      dependent.name,
                      dependent.version,
                      sequence,
                      function (error, result) {
                        /* istanbul ignore if */
                        if (error) {
                          done(error)
                        } else {
                          var publishedTree = tree.concat({
                            name: publishedName,
                            version: publishedVersion,
                            links: tree.map(function (dependency) {
                              return {
                                name: dependency.name,
                                version: dependency.version,
                                range: ranges[dependency.name]
                              }
                            })
                          })
                          var updated = update(
                            result.tree,
                            publishedName,
                            publishedVersion,
                            publishedTree
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
                      newDependentTrees.forEach(function (record) {
                        batch.push({
                          key: encode(
                            'tree',
                            record.name,
                            pack(sequence),
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

prototype._treeFor = function (
  sequence, name, version, ranges, callback
) {
  var self = this
  asyncMap(
    Object.keys(ranges).map(function (dependencyName) {
      return {
        name: dependencyName,
        range: ranges[dependencyName]
      }
    }),
    function (dependency, done) {
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
    function (error, dependencyTrees) {
      /* istanbul ignore if */
      if (error) {
        callback(error)
      } else {
        var combinedTree = []
        dependencyTrees.forEach(function (tree) {
          combinedTree = merge(combinedTree, tree)
        })
        callback(null, combinedTree)
      }
    }
  )
}

var ZERO = pack(0)

prototype._findMaxSatisfying = function (
  sequence, name, range, callback
) {
  this._findTrees(sequence, name, function (error, records) {
    /* istanbul ignore if */
    if (error) {
      callback(error)
    } else {
      var versions = records.map(function (record) {
        return record.version
      })
      var max = semver.maxSatisfying(versions, range)
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
        var completeTree = merge(matching.tree, treeWithDependency)
        callback(null, completeTree)
      }
    }
  })
}

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

prototype._findTrees = function (sequence, name, callback) {
  var matches = []
  this._levelup.createReadStream({
    gt: encode('tree', name, ZERO, ''),
    lt: encode('tree', name, pack(sequence), '~'),
    reverse: true
  })
  .once('error', /* istanbul ignore next */ function (error) {
    callback(error)
  })
  .on('data', function (data) {
    var decoded = decode(data.key)
    matches.push({
      version: decoded[3],
      sequence: unpack(decoded[2]),
      tree: data.value
    })
  })
  .once('end', function () {
    callback(null, matches)
  })
}

prototype._findDependents = function (
  sequence, name, version, callback
) {
  var matches = []
  this._levelup.createReadStream({
    gt: encode('dependent', name, ZERO, ''),
    lt: encode('dependent', name, pack(sequence), '~'),
    keys: true,
    values: false
  })
  .once('error', /* istanbul ignore next */ function (error) {
    callback(error)
  })
  .on('data', function (key) {
    var decoded = decode(key)
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
  .once('end', function () {
    callback(null, matches.filter(function (match) {
      return semver.satisfies(version, match.dependency.range)
    }))
  })
}

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

prototype.sequence = function () {
  return this._sequence
}

function encode (/* variadic */) {
  return Array.prototype.slice.call(arguments)
  .map(encodeURIComponent)
  .join('/')
}

function decode (key) {
  return key
  .split('/')
  .map(decodeURIComponent)
}

function pack (integer) {
  return lexint.pack(integer, 'hex')
}

function unpack (string) {
  return lexint.unpack(string, 'hex')
}
