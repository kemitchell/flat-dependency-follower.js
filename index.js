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
  Object.keys(chunk.versions).forEach(function (publishedVersion) {
    var dependencies = chunk.versions[publishedVersion].dependencies
    var dependencyPairs = []
    Object.keys(dependencies).forEach(function (dependencyName) {
      var dependencyRange = dependencies[dependencyName]
      dependencyPairs.push({
        name: dependencyName,
        range: dependencyRange
      })
      /*
      batch.push({
        type: 'put',
        key: encode(
          'dependency-dependent',
          dependencyName, dependencyRange,
          publishedName, publishedVersion,
          sequence
        )
      })
      batch.push({
        type: 'put',
        key: encode(
          'dependent-dependency',
          publishedName, publishedVersion,
          dependencyName, dependencyRange,
          sequence
        )
      })
      */
    })
    asyncMap(dependencyPairs, findTree, function (error, trees) {
      if (error) {
        callback(error)
      } else {
        var combinedTree = []
        trees.forEach(function (tree) {
          combinedTree = mergeTrees(combinedTree, tree)
        })
        combinedTree.sort(compareDependencies)
        batch.push({
          type: 'put',
          key: encode(
            'tree',
            publishedName,
            pack(sequence),
            publishedVersion
          ),
          value: combinedTree
        })
        // TODO: Write new dependents trees.
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
    })
    function findTree (pair, done) {
      var name = pair.name
      var range = pair.range
      self._findTrees(sequence, name, function (error, records) {
        if (error) {
          callback(error)
        } else if (records.length === 0) {
          done(new Error('no trees for ' + name + '@' + range))
        } else {
          var max = semver.maxSatisfying(
            records.map(function (record) {
              return record.version
            }),
            range
          )
          if (max === null) {
            done(
              new Error('no tree matching ' + name + '@' + range)
            )
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
            completeTree.sort(compareDependencies)
            done(null, completeTree)
          }
        }
      })
    }
  })
}

var ZERO = pack(0)

prototype._findTrees = function (sequence, name, callback) {
  var matches = []
  this._levelup.createReadStream({
    gt: encode('tree', name, ZERO, ''),
    lt: encode('tree', name, pack(sequence), '~')
  })
  .once('error', function (error) {
    callback(error)
  })
  .once('data', function (data) {
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
