#!/usr/bin/env node
var pino = require('pino')
var pull = require('pull-stream')
var registry = require('pull-npm-registry-updates')

var sink = require('./').sink
var sequence = require('./').sequence

var DIRECTORY = process.env.DIRECTORY || 'follower'
var log = pino({
  level: process.env.LOG_LEVEL.toLowerCase() || 'warn'
})

var IGNORE = process.env.IGNORE
  ? process.env.IGNORE
    .split(',')
    .map(function (string) {
      return string.trim()
    })
  : false

log.info({
  packages: IGNORE
}, 'ignoring')

sequence(DIRECTORY, function (error, since) {
  if (error && error.code !== 'ENOENT') {
    log.error(error)
    process.exit(1)
  } else {
    since = since || 0
    log.info({
      since: since,
      directory: DIRECTORY
    }, 'start')
    pull(
      registry({since: since}),
      pull.filter(function (update) {
        var ignore = IGNORE && IGNORE.indexOf(update.id) !== -1
        if (ignore) {
          log.info({
            sequence: update.seq,
            id: update.id
          }, 'ignored')
        }
        return !ignore
      }),
      function loggingSpy (source) {
        return function (end, callback) {
          source(end, function (end, update) {
            if (update && update.hasOwnProperty('seq')) {
              log.info({
                sequence: update.seq,
                id: update.id
              }, 'update')
            }
            callback(end, update)
          })
        }
      },
      sink(DIRECTORY, log, function (error) {
        if (error) {
          log.error(error)
          process.exit(1)
        }
        log.info('end')
      })
    )
  }
})
