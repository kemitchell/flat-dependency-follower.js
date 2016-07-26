var FlatDependencyFollower = require('./')
var from2Array = require('from2-array').obj
var memdb = require('memdb')
var runParallel = require('run-parallel')
var tape = require('tape')

tape('x -> y', function (test) {
  var follower = testFollower([
    {name: 'y', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}}
  ])
  .once('finish', function () {
    follower.query('x', '1.0.0', 2, function (error, tree, sequence) {
      test.ifError(error, 'no error')
      test.equal(sequence, 2, 'sequence is 2')
      test.deepEqual(
        tree,
        [{name: 'y', version: '1.0.0', range: '^1.0.0', links: []}],
        'yields tree'
      )
      test.end()
    })
  })
})

tape('x -> y -> z', function (test) {
  var follower = testFollower([
    {name: 'z', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'y', versions: {'1.0.0': {dependencies: {z: '^1.0.0'}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}}
  ])
  .once('finish', function () {
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
      test.end()
    })
  })
})

tape('w -> x -> y -> z ; new z', function (test) {
  var follower = testFollower([
    {name: 'z', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'y', versions: {'1.0.0': {dependencies: {z: '^1.0.0'}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}},
    {name: 'w', versions: {'1.0.0': {dependencies: {x: '^1.0.0'}}}},
    {name: 'z', versions: {'1.0.1': {dependencies: {}}}}
  ])
  .once('finish', function () {
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
      test.end()
    })
  })
})

tape('w -> x -> y -> z -> a; new y', function (test) {
  var follower = testFollower([
    {name: 'a', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'z', versions: {'1.0.0': {dependencies: {a: '^1.0.0'}}}},
    {name: 'y', versions: {'1.0.0': {dependencies: {z: '^1.0.0'}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}},
    {name: 'w', versions: {'1.0.0': {dependencies: {x: '^1.0.0'}}}},
    {name: 'y', versions: {'1.0.1': {dependencies: {z: '^1.0.0'}}}}
  ])
  .once('finish', function () {
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
      test.end()
    })
  })
})

tape('w -> x -> y -> z ; new y', function (test) {
  var follower = testFollower([
    {name: 'z', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'y', versions: {'1.0.0': {dependencies: {z: '^1.0.0'}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}},
    {name: 'w', versions: {'1.0.0': {dependencies: {x: '^1.0.0'}}}},
    {name: 'y', versions: {'1.0.1': {dependencies: {z: '^1.0.0'}}}}
  ])
  .once('finish', function () {
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
      test.end()
    })
  })
})

tape('x -> y -> z at earlier sequence', function (test) {
  var follower = testFollower([
    {name: 'z', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'y', versions: {'1.0.0': {dependencies: {z: '^1.0.0'}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}}
  ])
  .once('finish', function () {
    follower.query('x', '1.0.0', 2, function (error, tree, sequence) {
      test.ifError(error, 'no error')
      test.equal(tree, null, 'no tree')
      test.equal(sequence, null, 'no sequence')
      test.end()
    })
  })
})

tape('y@1.0.0 ; x -> y@^1.0.0 ; y@1.0.1', function (test) {
  var follower = testFollower([
    {name: 'y', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}},
    {name: 'y', versions: {'1.0.1': {dependencies: {}}}}
  ])
  .once('finish', function () {
    runParallel([
      function (done) {
        follower.query('x', '1.0.0', 2, function (error, tree) {
          test.ifError(error, 'no error')
          test.deepEqual(
            tree,
            [{name: 'y', version: '1.0.0', range: '^1.0.0', links: []}],
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
            [{name: 'y', version: '1.0.1', range: '^1.0.0', links: []}],
            'updated x depends on y@1.0.1'
          )
          done()
        })
      }
    ], function (error, done) {
      test.ifError(error, 'no error')
      test.end()
    })
  })
})

tape('no matching version', function (test) {
  testFollower([
    {name: 'y', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^2.0.0'}}}}
  ])
  .once('error', function (error) {
    test.strictEqual(error.noSatisfying, true)
    test.strictEqual(error.sequence, 2)
    test.deepEqual(error.dependent, {name: 'x', version: '1.0.0'})
    test.deepEqual(error.dependency, {name: 'y', range: '^2.0.0'})
    test.strictEqual(
      error.message, 'no package satisfying y@^2.0.0 for x@1.0.0'
    )
    test.end()
  })
})

tape('sequence number', function (test) {
  var follower = testFollower([
    {name: 'y', versions: {'1.0.0': {dependencies: {}}}},
    {name: 'x', versions: {'1.0.0': {dependencies: {y: '^1.0.0'}}}},
    {name: 'y', versions: {'1.0.1': {dependencies: {}}}}
  ])
  .once('finish', function () {
    test.equal(follower.sequence(), 3)
    test.end()
  })
})

function testFollower (updates) {
  var store = memdb({valueEncoding: 'json'})
  var follower = Math.random() > 0.5
  ? new FlatDependencyFollower(store)
  : FlatDependencyFollower(store)
  updates.forEach(function (update, index) {
    update.sequence = index + 1
  })
  return from2Array(updates).pipe(follower)
}
