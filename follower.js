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
      function loggingSpy (source) {
        return function (end, callback) {
          source(end, function (end, update) {
            if (update && update.hasOwnProperty('sequence')) {
              log.info({
                sequence: update.sequence,
                name: update.name
              }, 'update')
            }
            callback(end, update)
          })
        }
      },
      sink(DIRECTORY, function (error) {
        if (error) {
          log.error(error)
          process.exit(1)
        }
        log.info('end')
      })
    )
  }
})
