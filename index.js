var Writable = require('stream').Writable
var find = require('array-find')
var asyncMap = require('async.map')
var inherits = require('util').inherits
var lexint = require('lexicographic-integer')
var semver = require('semver')

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
      if (!operation.hasOwnProperty('type')) {
        operation.type = 'put'
        if (!operation.hasOwnProperty('value')) {
          operation.value = ''
        }
      }
    })
    console.log('%s is %j\n\n', 'batch', batch)
    self._levelup.batch(batch, function (error) {
      if (error) {
        callback(error)
      } else {
        if (sequence > self._sequence) {
          self._sequence = sequence
        }
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
        if (error) {
          callback(error)
        } else {
          console.log('%s is %j', 'tree', tree)
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
              console.log(
                '%s of %s@%s are %j',
                'dependents', publishedName, publishedVersion, dependents
              )
              if (error) {
                callback(error)
              } else {
                asyncMap(
                  dependents,
                  function (record, done) {
                    var dependent = record.dependent
                    self._findRanges(
                      dependent.name, dependent.version, sequence,
                      function (error, ranges) {
                        if (error) {
                          callback(error)
                        } else {
                          if (ranges === null) {
                            callback(
                              new Error(
                                'no ranges for ' +
                                dependent.name + '@' + dependent.version
                              )
                            )
                          } else {
                            self._treeFor(
                              sequence,
                              dependent.name,
                              dependent.version,
                              ranges,
                              function (error, newTree) {
                                console.log('%s is %j', 'newTree', newTree)
                                if (error) {
                                  done(error)
                                } else {
                                  done(null, {
                                    name: dependent.name,
                                    version: dependent.version,
                                    tree: newTree
                                  })
                                }
                              }
                            )
                          }
                        }
                      }
                    )
                  },
                  function (error, newDependentTrees) {
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
        sequence, dependency.name, dependency.range, done
      )
    },
    function (error, dependencyTrees) {
      if (error) {
        callback(error)
      } else {
        var combinedTree = []
        dependencyTrees.forEach(function (tree) {
          combinedTree = mergeTrees(combinedTree, tree)
        })
        combinedTree.sort(compareDependencies)
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
    if (error) {
      callback(error)
    } else {
      var versions = records.map(function (record) {
        return record.version
      })
      var max = semver.maxSatisfying(versions, range)
      if (max === null) {
        callback(null, null)
      } else {
        var matching = find(records, function (record) {
          return record.version === max
        })
        var completeTree = mergeTrees(
          matching.tree,
          [
            {
              name: name,
              version: max,
              links: matching.tree.map(function (dependency) {
                return {
                  name: dependency.name,
                  version: dependency.version
                }
              })
            }
          ]
        )
        callback(null, completeTree)
      }
    }
  })
}

prototype._findRanges = function (name, version, sequence, callback) {
  var found = false
  this._levelup.createReadStream({
    gte: encode('ranges', name, version, ZERO),
    lte: encode('ranges', name, version, pack(sequence)),
    limit: 1,
    reverse: true,
    keys: false,
    values: true
  })
  .once('error', function (error) {
    callback(error)
  })
  .once('data', function (value) {
    found = true
    callback(null, value)
  })
  .once('end', function () {
    if (!found) {
      callback(null, null)
    }
  })
}

prototype._findTrees = function (sequence, name, callback) {
  var matches = []
  this._levelup.createReadStream({
    gt: encode('tree', name, ZERO, ''),
    lt: encode('tree', name, pack(sequence), '~')
  })
  .once('error', function (error) {
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

prototype._getTree = function (name, sequence, version, callback) {
  var key = encode('tree', name, pack(sequence), version)
  this._levelup.get(key, function (error, tree) {
    if (error) {
      if (error.notFound) {
        callback(null, null)
      } else {
        callback(error)
      }
    } else {
      callback(null, tree)
    }
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
  .once('error', function (error) {
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
    console.log('%s is %j', 'matches', matches)
    callback(null, matches.filter(function (match) {
      return semver.satisfies(version, match.dependency.range)
    }))
  })
}

prototype.query = function (name, version, sequence, callback) {
  this._findTrees(sequence, name, function (error, matches) {
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

function mergeTrees (a, b) {
  var merged = [].concat(a)
  b.forEach(function (element) {
    var alreadyHave = merged.some(function (have) {
      return (
        have.name === element.name &&
        have.version === element.version
      )
    })
    if (!alreadyHave) {
      merged.push(element)
    }
  })
  return merged
}

function compareDependencies (a, b) {
  if (a.name < b.name) {
    return -1
  } else if (a.name > b.name) {
    return 1
  } else {
    return semver.compare(a.version, b.version)
  }
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
