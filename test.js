var AbstractBlobStore = require('abstract-blob-store')
var FlatDependencyFollower = require('./')
var from2Array = require('from2-array').obj
var runParallel = require('run-parallel')
var tape = require('tape')

tape('x -> y', function (test) {
  testFollower(test, [
    {name: 'y', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}}
  ], function (follower, done) {
    follower.once('finish', function () {
      follower.query('x', '1.0.0', 2, function (error, tree, sequence) {
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
})

tape('y@2; y@10; x -> y@*', function (test) {
  testFollower(test, [
    // Lexicographically: 2.0.0 > 10.0.0
    // SemVer:            2.0.0 < 10.0.0
    {name: 'y', versions: {'2.0.0': {dependencies: {}}}},
    {name: 'y', versions: {'10.0.0': {dependencies: {}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '*'}}}}
  ], function (follower, done) {
    follower.once('finish', function () {
      follower.query('x', '1.0.0', 3, function (error, tree, sequence) {
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
})

tape('x -> y -> z', function (test) {
  testFollower(test, [
    {name: 'z', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'y', versions: {'1.0.0': {dependencies: {z: '^1.0.0'}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}}
  ], function (follower, done) {
    follower.once('finish', function () {
      follower.query('x', '1.0.0', 3, function (error, tree, sequence) {
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
})

tape('w -> x -> y -> z ; new z', function (test) {
  testFollower(test, [
    {name: 'z', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'y', versions: {'1.0.0': {dependencies: {z: '^1.0.0'}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}},
    {name: 'w', versions: {'1.0.0': {dependencies: {x: '^1.0.0'}}}},
    {name: 'z', versions: {'1.0.1': {dependencies: {}}}}
  ], function (follower, done) {
    follower.once('finish', function () {
      follower.query('w', '1.0.0', 5, function (error, tree, sequence) {
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
})

tape('w -> x -> y -> z -> a; new y', function (test) {
  testFollower(test, [
    {name: 'a', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'z', versions: {'1.0.0': {dependencies: {a: '^1.0.0'}}}},
    {name: 'y', versions: {'1.0.0': {dependencies: {z: '^1.0.0'}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}},
    {name: 'w', versions: {'1.0.0': {dependencies: {x: '^1.0.0'}}}},
    {name: 'y', versions: {'1.0.1': {dependencies: {z: '^1.0.0'}}}}
  ], function (follower, done) {
    follower.once('finish', function () {
      follower.query('w', '1.0.0', 6, function (error, tree, sequence) {
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
})

tape('w -> x -> y -> z ; new y', function (test) {
  testFollower(test, [
    {name: 'z', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'y', versions: {'1.0.0': {dependencies: {z: '^1.0.0'}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}},
    {name: 'w', versions: {'1.0.0': {dependencies: {x: '^1.0.0'}}}},
    {name: 'y', versions: {'1.0.1': {dependencies: {z: '^1.0.0'}}}}
  ], function (follower, done) {
    follower.once('finish', function () {
      runParallel([
        function (done) {
          follower.query(
            'w', '1.0.0', 5,
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
          follower.query(
            'x', '1.0.0', 5,
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
          follower.query(
            'y', '1.0.1', 5,
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
          follower.query(
            'z', '1.0.0', 5,
            function (error, tree, sequence) {
              test.ifError(error, 'no error')
              test.equal(sequence, 1, 'sequence is 1')
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
})

tape('x -> y -> z at earlier sequence', function (test) {
  testFollower(test, [
    {name: 'z', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'y', versions: {'1.0.0': {dependencies: {z: '^1.0.0'}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}}
  ], function (follower, done) {
    follower.once('finish', function () {
      follower.query('x', '1.0.0', 2, function (error, tree, sequence) {
        test.ifError(error, 'no error')
        test.equal(tree, null, 'no tree')
        test.equal(sequence, null, 'no sequence')
        done()
      })
    })
  })
})

tape('y@1.0.0 ; x -> y@^1.0.0 ; y@1.0.1', function (test) {
  testFollower(test, [
    {name: 'y', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}},
    {name: 'y', versions: {'1.0.1': {dependencies: {}}}}
  ], function (follower, done) {
    follower.once('finish', function () {
      runParallel([
        function (done) {
          follower.query('x', '1.0.0', 2, function (error, tree) {
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
          follower.query('x', '1.0.0', 3, function (error, tree) {
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
  ], function (follower, done) {
    follower.once('finish', function () {
      follower.query('x', '1.0.0', 2, function (error, tree) {
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
})

tape('x@1 -> y@1; x@2 -> y@2', function (test) {
  testFollower(test, [
    {name: 'y', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}},
    {name: 'y', versions: {'2.0.0': {dependencies: {}}}},
    {name: 'x', versions: {'2.0.0': {dependencies: {y: '^2.0.0'}}}}
  ], function (follower, done) {
    follower.once('finish', function () {
      follower.query('x', '2.0.0', 4, function (error, tree, sequence) {
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
})

tape('no matching version', function (test) {
  testFollower(test, [
    {name: 'y', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^2.0.0'}}}}
  ], function (follower, done) {
    follower.once('missing', function (missing) {
      test.strictEqual(missing.sequence, 2)
      test.deepEqual(missing.dependent, {name: 'x', version: '1.0.0'})
      test.deepEqual(missing.dependency, {name: 'y', range: '^2.0.0'})
      test.strictEqual(
        missing.message, 'no package satisfying y@^2.0.0 for x@1.0.0'
      )
      done()
    })
  })
})

tape('versions(existing)', function (test) {
  testFollower(test, [
    {name: 'x', versions: {'1.0.0': {}, '2.0.0': {}}}
  ], function (follower, done) {
    follower.once('finish', function () {
      follower.versions('x', function (error, versions) {
        test.ifError(error)
        test.deepEqual(
          versions, ['1.0.0', '2.0.0'],
          'yields versions array'
        )
      })
      done()
    })
  })
})

tape('versions after multiple updates', function (test) {
  testFollower(test, [
    {name: 'x', versions: {'1.0.0': {}}},
    {name: 'x', versions: {'1.0.0': {}, '2.0.0': {}}}
  ], function (follower, done) {
    follower.once('finish', function () {
      follower.versions('x', function (error, versions) {
        test.ifError(error)
        test.deepEqual(
          versions, ['1.0.0', '2.0.0'],
          'yields versions array'
        )
        done()
      })
    })
  })
})

tape('versions(unknown)', function (test) {
  testFollower(test, [
    {name: 'x', versions: {'1.0.0': {}, '2.0.0': {}}}
  ], function (follower, done) {
    follower.once('finish', function () {
      follower.versions('y', function (error, versions) {
        test.ifError(error)
        test.deepEqual(versions, null, 'yields null')
      })
      done()
    })
  })
})

tape('list package names', function (test) {
  testFollower(test, [
    {name: 'x', versions: {'1.0.0': {}}},
    {name: 'y', versions: {'1.0.0': {}}},
    {name: 'z', versions: {'1.0.0': {}}}
  ], function (follower, done) {
    follower.once('finish', function () {
      var buffer = []
      follower.packages()
      .on('data', function (chunk) {
        buffer.push(chunk)
      })
      .once('end', function () {
        test.deepEqual(buffer, ['x', 'y', 'z'], 'streams names')
        done()
      })
    })
  })
})

tape('dependency appears later', function (test) {
  testFollower(test, [
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}},
    {name: 'y', versions: {'1.0.0': {dependencies: {}}}}
  ], function (follower, done) {
    follower.once('finish', function () {
      runParallel([
        function (done) {
          follower.query('x', '1.0.0', 1, function (error, tree) {
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
          follower.query('x', '1.0.0', 2, function (error, tree) {
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
})

tape('sequence number', function (test) {
  testFollower(test, [
    {name: 'y', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}},
    {name: 'y', versions: {'1.0.1': {dependencies: {}}}}
  ], function (follower, done) {
    follower.once('finish', function () {
      test.equal(follower.sequence(), 3)
      done()
    })
  })
})

tape('no dependencies object', function (test) {
  testFollower(test, [
    {name: 'y', versions: {'1.0.0': {}}}
  ], function (follower, done) {
    follower.once('finish', function () {
      done()
    })
  })
})

tape('non-publish update', function (test) {
  testFollower(test, [
    {},
    {name: 'y', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}}
  ], function (follower, done) {
    follower.once('finish', function () {
      follower.query('x', '1.0.0', 3, function (error, tree, sequence) {
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
  ], function (follower, done) {
    follower.once('finish', function () {
      follower.query('y', '1.0.0', 2, function (error, tree, sequence) {
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
})

function testFollower (test, updates, callback) {
  var store = new AbstractBlobStore()
  var follower = Math.random() > 0.5
    ? new FlatDependencyFollower(store)
    : FlatDependencyFollower(store)
  from2Array(
    updates.map(function (update, index) {
      return {
        seq: index + 1,
        doc: update
      }
    })
  )
  .pipe(follower)
  callback(follower, function () {
    test.end()
  })
}
