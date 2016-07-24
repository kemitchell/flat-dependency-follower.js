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

function testFollower (updates) {
  var store = memdb({valueEncoding: 'json'})
  var follower = new FlatDependencyFollower(store)
  updates.forEach(function (update, index) {
    update.sequence = index + 1
  })
  return from2Array(updates).pipe(follower)
}
