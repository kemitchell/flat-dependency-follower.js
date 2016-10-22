var packages = require('./').packages
var pull = require('pull-stream')
var query = require('./').query
var runParallel = require('run-parallel')
var sink = require('./').sink
var tape = require('tape')
var temporaryDirectory = require('temporary-directory')
var values = require('pull-stream').values
var versions = require('./').versions

tape('x -> y', function (test) {
  testFollower(test, [
    {name: 'y', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}}
  ], function (dir, done) {
    query(dir, 'x', '1.0.0', 2, function (error, tree, seq) {
      test.ifError(error, 'no error')
      test.equal(seq, 2, 'sequence is 2')
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
  ], function (dir, done) {
    query(dir, 'x', '1.0.0', 3, function (error, tree, seq) {
      test.ifError(error, 'no error')
      test.equal(seq, 3, 'sequence is 3')
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
  ], function (dir, done) {
    query(dir, 'x', '1.0.0', 3, function (error, tree, seq) {
      test.ifError(error, 'no error')
      test.equal(seq, 3, 'sequence is 3')
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
  ], function (dir, done) {
    query(dir, 'w', '1.0.0', 5, function (error, tree, seq) {
      test.ifError(error, 'no error')
      test.equal(seq, 5, 'sequence is 5')
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
  ], function (dir, done) {
    query(dir, 'w', '1.0.0', 6, function (error, tree, seq) {
      test.ifError(error, 'no error')
      test.equal(seq, 6, 'sequence is 6')
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
  ], function (dir, done) {
    runParallel([
      function (done) {
        query(
          dir, 'w', '1.0.0', 5,
          function (error, tree, seq) {
            test.ifError(error, 'no error')
            test.equal(seq, 5, 'sequence is 5')
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
        query(
          dir, 'x', '1.0.0', 5,
          function (error, tree, seq) {
            test.ifError(error, 'no error')
            test.equal(seq, 5, 'sequence is 5')
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
        query(
          dir, 'y', '1.0.1', 5,
          function (error, tree, seq) {
            test.ifError(error, 'no error')
            test.equal(seq, 5, 'sequence is 5')
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
        query(
          dir, 'z', '1.0.0', 5,
          function (error, tree, seq) {
            test.ifError(error, 'no error')
            test.equal(seq, 1, 'sequence is 1')
            test.deepEqual(tree, [], 'yields tree')
            done()
          }
        )
      }
    ], function (error) {
      test.ifError(error)
      done()
    })
  })
})

tape('x -> y -> z at earlier sequence', function (test) {
  testFollower(test, [
    {name: 'z', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'y', versions: {'1.0.0': {dependencies: {z: '^1.0.0'}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}}
  ], function (dir, done) {
    query(dir, 'x', '1.0.0', 2, function (error, tree, seq) {
      test.ifError(error, 'no error')
      test.equal(tree, null, 'no tree')
      test.equal(seq, null, 'no sequence')
      done()
    })
  })
})

tape('y@1.0.0 ; x -> y@^1.0.0 ; y@1.0.1', function (test) {
  testFollower(test, [
    {name: 'y', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}},
    {name: 'y', versions: {'1.0.1': {dependencies: {}}}}
  ], function (dir, done) {
    runParallel([
      function (done) {
        query(dir, 'x', '1.0.0', 2, function (error, tree) {
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
        query(dir, 'x', '1.0.0', 3, function (error, tree) {
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
  ], function (dir, done) {
    query(dir, 'x', '1.0.0', 2, function (error, tree) {
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
  ], function (dir, done) {
    query(dir, 'x', '2.0.0', 4, function (error, tree, seq) {
      test.ifError(error, 'no error')
      test.equal(seq, 4, 'sequence is 4')
      test.deepEqual(
        tree,
        [{name: 'y', version: '2.0.0', range: '^2.0.0', links: []}],
        'yields tree'
      )
      done()
    })
  })
})

tape.only('versions(existing)', function (test) {
  testFollower(test, [
    {name: 'x', versions: {'1.0.0': {}, '2.0.0': {}}}
  ], function (dir, done) {
    versions(dir, 'x', function (error, versions) {
      test.ifError(error)
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
  ], function (dir, done) {
    versions(dir, 'x', function (error, versions) {
      test.ifError(error)
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
  ], function (dir, done) {
    versions(dir, 'y', function (error, versions) {
      test.ifError(error)
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
  ], function (dir, done) {
    pull(
      packages(),
      pull.collect(function (collected) {
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
  ], function (dir, done) {
    runParallel([
      function (done) {
        query(dir, 'x', '1.0.0', 1, function (error, tree) {
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
        query(dir, 'x', '1.0.0', 2, function (error, tree) {
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
  ], function (dir, done) {
    done()
  })
})

tape('non-publish update', function (test) {
  testFollower(test, [
    {},
    {name: 'y', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}}
  ], function (dir, done) {
    query(dir, 'x', '1.0.0', 3, function (error, tree, seq) {
      test.ifError(error, 'no error')
      test.equal(seq, 3, 'sequence is 3')
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
  ], function (dir, done) {
    query(dir, 'y', '1.0.0', 2, function (error, tree, seq) {
      test.ifError(error, 'no error')
      test.equal(seq, 2, 'sequence is 2')
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
      sink(directory, function (error) {
        test.ifError(error, 'no pipeline error')
        callback(directory, function () {
          removeDirectory()
          test.end()
        })
      })
    )
  })
}
