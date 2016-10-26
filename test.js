var pino = require('pino')
var pull = require('pull-stream')
var runParallel = require('run-parallel')
var tape = require('tape')
var temporaryDirectory = require('temporary-directory')
var values = require('pull-stream').values

var packages = require('./').packages
var tree = require('./').tree
var sink = require('./').sink
var versions = require('./').versions

tape('x -> y', function (test) {
  testFollower(test, [
    {name: 'y', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}}
  ], function (directory, done) {
    tree(directory, 'x', '1.0.0', 2, function (error, tree, sequence) {
      test.ifError(error, 'no error')
      test.equal(sequence, 2, 'sequence is 2')
      test.deepEqual(
        tree,
        [{name: 'y', version: '1.0.0', range: '^1.0.0', links: []}],
        'yields tree'
      )
      done()
    })
  })
})

tape('y@2; y@10; x -> y@*', function (test) {
  testFollower(test, [
    // Lexicographically: 2.0.0 > 10.0.0
    // SemVer:            2.0.0 < 10.0.0
    {name: 'y', versions: {'2.0.0': {dependencies: {}}}},
    {name: 'y', versions: {'10.0.0': {dependencies: {}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '*'}}}}
  ], function (directory, done) {
    tree(directory, 'x', '1.0.0', 3, function (error, tree, sequence) {
      test.ifError(error, 'no error')
      test.equal(sequence, 3, 'sequence is 3')
      test.deepEqual(
        tree,
        [{name: 'y', version: '10.0.0', range: '*', links: []}],
        'yields tree'
      )
      done()
    })
  })
})

tape('x -> y -> z', function (test) {
  testFollower(test, [
    {name: 'z', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'y', versions: {'1.0.0': {dependencies: {z: '^1.0.0'}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}}
  ], function (directory, done) {
    tree(directory, 'x', '1.0.0', 3, function (error, tree, sequence) {
      test.ifError(error, 'no error')
      test.equal(sequence, 3, 'sequence is 3')
      test.deepEqual(
        tree,
        [
          {
            name: 'y',
            version: '1.0.0',
            range: '^1.0.0',
            links: [{name: 'z', version: '1.0.0', range: '^1.0.0'}]
          },
          {name: 'z', version: '1.0.0', links: []}
        ],
        'yields tree'
      )
      done()
    })
  })
})

tape('w -> x -> y -> z ; new z', function (test) {
  testFollower(test, [
    {name: 'z', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'y', versions: {'1.0.0': {dependencies: {z: '^1.0.0'}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}},
    {name: 'w', versions: {'1.0.0': {dependencies: {x: '^1.0.0'}}}},
    {name: 'z', versions: {'1.0.1': {dependencies: {}}}}
  ], function (directory, done) {
    tree(directory, 'w', '1.0.0', 5, function (error, tree, sequence) {
      test.ifError(error, 'no error')
      test.equal(sequence, 5, 'sequence is 5')
      test.deepEqual(
        tree,
        [
          {
            name: 'x',
            version: '1.0.0',
            range: '^1.0.0',
            links: [{name: 'y', version: '1.0.0', range: '^1.0.0'}]
          },
          {
            name: 'y',
            version: '1.0.0',
            links: [{name: 'z', version: '1.0.1', range: '^1.0.0'}]
          },
          {name: 'z', version: '1.0.1', links: []}
        ],
        'yields tree'
      )
      done()
    })
  })
})

tape('w -> x -> y -> z -> a; new y', function (test) {
  testFollower(test, [
    {name: 'a', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'z', versions: {'1.0.0': {dependencies: {a: '^1.0.0'}}}},
    {name: 'y', versions: {'1.0.0': {dependencies: {z: '^1.0.0'}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}},
    {name: 'w', versions: {'1.0.0': {dependencies: {x: '^1.0.0'}}}},
    {name: 'y', versions: {'1.0.1': {dependencies: {z: '^1.0.0'}}}}
  ], function (directory, done) {
    tree(directory, 'w', '1.0.0', 6, function (error, tree, sequence) {
      test.ifError(error, 'no error')
      test.equal(sequence, 6, 'sequence is 6')
      test.deepEqual(
        tree,
        [
          {
            name: 'a',
            version: '1.0.0',
            links: []
          },
          {
            name: 'x',
            version: '1.0.0',
            range: '^1.0.0',
            links: [{name: 'y', version: '1.0.1', range: '^1.0.0'}]
          },
          {
            name: 'y',
            version: '1.0.1',
            links: [{name: 'z', version: '1.0.0', range: '^1.0.0'}]
          },
          {
            name: 'z',
            version: '1.0.0',
            links: [{name: 'a', version: '1.0.0', range: '^1.0.0'}]
          }
        ],
        'yields tree'
      )
      done()
    })
  })
})

tape('w -> x -> y -> z ; new y', function (test) {
  testFollower(test, [
    {name: 'z', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'y', versions: {'1.0.0': {dependencies: {z: '^1.0.0'}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}},
    {name: 'w', versions: {'1.0.0': {dependencies: {x: '^1.0.0'}}}},
    {name: 'y', versions: {'1.0.1': {dependencies: {z: '^1.0.0'}}}}
  ], function (directory, done) {
    runParallel([
      function (done) {
        tree(
          directory, 'w', '1.0.0', 5,
          function (error, tree, sequence) {
            test.ifError(error, 'no error')
            test.equal(sequence, 5, 'sequence is 5')
            test.deepEqual(
              tree,
              [
                {
                  name: 'x',
                  version: '1.0.0',
                  range: '^1.0.0',
                  links: [
                    {name: 'y', version: '1.0.1', range: '^1.0.0'}
                  ]
                },
                {
                  name: 'y',
                  version: '1.0.1',
                  links: [
                    {name: 'z', version: '1.0.0', range: '^1.0.0'}
                  ]
                },
                {name: 'z', version: '1.0.0', links: []}
              ],
              'yields tree'
            )
            done()
          }
        )
      },
      function (done) {
        tree(
          directory, 'x', '1.0.0', 5,
          function (error, tree, sequence) {
            test.ifError(error, 'no error')
            test.equal(sequence, 5, 'sequence is 5')
            test.deepEqual(
              tree,
              [
                {
                  name: 'y',
                  version: '1.0.1',
                  range: '^1.0.0',
                  links: [
                    {name: 'z', version: '1.0.0', range: '^1.0.0'}
                  ]
                },
                {name: 'z', version: '1.0.0', links: []}
              ],
              'yields tree'
            )
            done()
          }
        )
      },
      function (done) {
        tree(
          directory, 'y', '1.0.1', 5,
          function (error, tree, sequence) {
            test.ifError(error, 'no error')
            test.equal(sequence, 5, 'sequence is 5')
            test.deepEqual(
              tree,
              [
                {
                  name: 'z',
                  version: '1.0.0',
                  range: '^1.0.0',
                  links: []
                }
              ],
              'yields tree'
            )
            done()
          }
        )
      },
      function (done) {
        tree(
          directory, 'z', '1.0.0', 5,
          function (error, tree, sequence) {
            test.ifError(error, 'no error')
            test.equal(sequence, 1, 'sequence is 1')
            test.deepEqual(tree, [], 'yields tree')
            done()
          }
        )
      }
    ], function (error) {
      test.ifError(error, 'no error')
      done()
    })
  })
})

tape('x -> y -> z at earlier sequence', function (test) {
  testFollower(test, [
    {name: 'z', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'y', versions: {'1.0.0': {dependencies: {z: '^1.0.0'}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}}
  ], function (directory, done) {
    tree(directory, 'x', '1.0.0', 2, function (error, tree, sequence) {
      test.ifError(error, 'no error')
      test.equal(tree, null, 'no tree')
      test.equal(sequence, null, 'no sequence')
      done()
    })
  })
})

tape('y@1.0.0 ; x -> y@^1.0.0 ; y@1.0.1', function (test) {
  testFollower(test, [
    {name: 'y', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}},
    {name: 'y', versions: {'1.0.1': {dependencies: {}}}}
  ], function (directory, done) {
    runParallel([
      function (done) {
        tree(directory, 'x', '1.0.0', 2, function (error, tree) {
          test.ifError(error, 'no error')
          test.deepEqual(
            tree,
            [
              {
                name: 'y',
                version: '1.0.0',
                range: '^1.0.0',
                links: []
              }
            ],
            'original x depends on y@1.0.0'
          )
          done()
        })
      },
      function (done) {
        tree(directory, 'x', '1.0.0', 3, function (error, tree) {
          test.ifError(error, 'no error')
          test.deepEqual(
            tree,
            [
              {
                name: 'y',
                version: '1.0.1',
                range: '^1.0.0',
                links: []
              }
            ],
            'updated x depends on y@1.0.1'
          )
          done()
        })
      }
    ], function (error) {
      test.ifError(error, 'no error')
      done()
    })
  })
})

tape('y@1.0.0 ; x -> y@^1.0.0 and z@git', function (test) {
  testFollower(test, [
    {name: 'y', versions: {'1.0.0': {dependencies: {}}}},
    {
      name: 'x',
      versions: {
        '1.0.0': {
          dependencies: {
            y: '^1.0.0',
            z: 'example/example'
          }
        }
      }
    }
  ], function (directory, done) {
    tree(directory, 'x', '1.0.0', 2, function (error, tree) {
      test.ifError(error, 'no error')
      test.deepEqual(
        tree,
        [
          {name: 'y', version: '1.0.0', range: '^1.0.0', links: []},
          {
            name: 'z',
            version: 'example/example',
            range: null,
            links: []
          }
        ],
        'range and Git deps'
      )
      done()
    })
  })
})

tape('x@1 -> y@1; x@2 -> y@2', function (test) {
  testFollower(test, [
    {name: 'y', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}},
    {name: 'y', versions: {'2.0.0': {dependencies: {}}}},
    {name: 'x', versions: {'2.0.0': {dependencies: {y: '^2.0.0'}}}}
  ], function (directory, done) {
    tree(directory, 'x', '2.0.0', 4, function (error, tree, sequence) {
      test.ifError(error, 'no error')
      test.equal(sequence, 4, 'sequence is 4')
      test.deepEqual(
        tree,
        [{name: 'y', version: '2.0.0', range: '^2.0.0', links: []}],
        'yields tree'
      )
      done()
    })
  })
})

tape('versions(existing)', function (test) {
  testFollower(test, [
    {name: 'x', versions: {'1.0.0': {}, '2.0.0': {}}}
  ], function (directory, done) {
    versions(directory, 'x', function (error, versions) {
      test.ifError(error, 'no error')
      test.deepEqual(
        versions, ['1.0.0', '2.0.0'],
        'yields versions array'
      )
    })
    done()
  })
})

tape('versions after multiple updates', function (test) {
  testFollower(test, [
    {name: 'x', versions: {'1.0.0': {}}},
    {name: 'x', versions: {'1.0.0': {}, '2.0.0': {}}}
  ], function (directory, done) {
    versions(directory, 'x', function (error, versions) {
      test.ifError(error, 'no error')
      test.deepEqual(
        versions, ['1.0.0', '2.0.0'],
        'yields versions array'
      )
      done()
    })
  })
})

tape('versions(unknown)', function (test) {
  testFollower(test, [
    {name: 'x', versions: {'1.0.0': {}, '2.0.0': {}}}
  ], function (directory, done) {
    versions(directory, 'y', function (error, versions) {
      test.ifError(error, 'no error')
      test.deepEqual(versions, null, 'yields null')
    })
    done()
  })
})

tape('list package names', function (test) {
  testFollower(test, [
    {name: 'x', versions: {'1.0.0': {}}},
    {name: 'y', versions: {'1.0.0': {}}},
    {name: 'z', versions: {'1.0.0': {}}}
  ], function (directory, done) {
    pull(
      packages(directory),
      pull.collect(function (error, collected) {
        test.ifError(error, 'no collect error')
        test.deepEqual(collected, ['x', 'y', 'z'], 'streams names')
        done()
      })
    )
  })
})

tape('dependency appears later', function (test) {
  testFollower(test, [
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}},
    {name: 'y', versions: {'1.0.0': {dependencies: {}}}}
  ], function (directory, done) {
    runParallel([
      function (done) {
        tree(directory, 'x', '1.0.0', 1, function (error, tree) {
          test.ifError(error, 'no error')
          test.deepEqual(
            tree,
            [{name: 'y', range: '^1.0.0', links: [], missing: true}],
            'with error'
          )
          done()
        })
      },
      function (done) {
        tree(directory, 'x', '1.0.0', 2, function (error, tree) {
          test.ifError(error, 'no error')
          test.deepEqual(
            tree,
            [
              {
                name: 'y',
                version: '1.0.0',
                range: '^1.0.0',
                links: []
              }
            ],
            'updated x depends on y@1.0.1'
          )
          done()
        })
      }
    ], function (error) {
      test.ifError(error, 'no error')
      done()
    })
  })
})

tape('no dependencies object', function (test) {
  testFollower(test, [
    {name: 'y', versions: {'1.0.0': {}}}
  ], function (directory, done) {
    done()
  })
})

tape('non-publish update', function (test) {
  testFollower(test, [
    {},
    {name: 'y', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}}
  ], function (directory, done) {
    tree(directory, 'x', '1.0.0', 3, function (error, tree, sequence) {
      test.ifError(error, 'no error')
      test.equal(sequence, 3, 'sequence is 3')
      test.deepEqual(
        tree,
        [{name: 'y', version: '1.0.0', range: '^1.0.0', links: []}],
        'yields tree'
      )
      done()
    })
  })
})

tape('malformed dependencies object', function (test) {
  testFollower(test, [
    {name: 'x', versions: {'1.0.0': {dependencies: {}}}},
    {
      name: 'y',
      versions: {
        '1.0.0': {
          dependencies: {x: {version: '1.0.0'}} // Invalid
        }
      }
    }
  ], function (directory, done) {
    tree(directory, 'y', '1.0.0', 2, function (error, tree, sequence) {
      test.ifError(error, 'no error')
      test.equal(sequence, 2, 'sequence is 2')
      test.deepEqual(
        tree,
        [{name: 'x', version: 'INVALID', range: null, links: []}],
        'yields tree'
      )
      done()
    })
  })
})

function testFollower (test, updates, callback) {
  temporaryDirectory(function (error, directory, removeDirectory) {
    test.ifError(error, 'no error creating test directory')
    pull(
      values(updates.map(function (update, index) {
        return {
          seq: index + 1,
          doc: update
        }
      })),
      sink(directory, pino({enabled: false}), function (error) {
        test.ifError(error, 'no pipeline error')
        callback(directory, function () {
          removeDirectory()
          test.end()
        })
      })
    )
  })
}
