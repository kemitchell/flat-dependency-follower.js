#!/usr/bin/env node
var ecb = require('ecb')
var http = require('http')
var https = require('https')
var parse = require('json-parse-errback')
var pino = require('pino')
var pull = require('pull-stream')
var to = require('stream-to-pull-stream')
var url = require('url')

var sequence = require('./').sequence
var tree = require('./').tree
var packages = require('./').packages
var versions = require('./').versions

var DIRECTORY = process.env.DIRECTORY || 'follower'
var log = pino({
  level: process.env.LOG_LEVEL
    ? process.env.LOG_LEVEL.toLowerCase()
    : 'warn'
})

log.info('start')

var PACKAGE_PATH = new RegExp(
  '^' +
  '/packages' +
  '/([^/]+)' + // package name
  '(/([^/]+))?' + // optional package version
  '(/([1-9][0-9]+))?' + // optional sequence
  '$'
)

var server = http.createServer(function (request, response) {
  if (request.method !== 'GET') {
    response.statusCode = 405
    response.end()
    return
  }

  var pathname = url.parse(request.url).pathname
  if (pathname === '/sequence') {
    sequence(DIRECTORY, ecb(internalError, function (sequence) {
      response.end(JSON.stringify(sequence))
    }))
  } else if (pathname === '/behind') {
    https.get('https://replicate.npmjs.com', function (response) {
      var buffer = []
      response
        .once('error', internalError)
        .on('data', function (chunk) {
          buffer.push(chunk)
        })
        .once('end', function () {
          var body = Buffer.concat(buffer)
          parse(body, ecb(internalError, function (body) {
            sequence(DIRECTORY, ecb(internalError, function (sequence) {
              sendJSON(body.update_seq - sequence)
            }))
          }))
        })
    })
  } else if (pathname === '/packages') {
    pull(
      packages(DIRECTORY),
      pull.map(function (name) {
        return name + '\n'
      }),
      to.sink(response)
    )
  } else if (pathname.indexOf('/packages/') === 0) {
    var match = PACKAGE_PATH.exec(pathname)
    if (match) {
      var name = decodeURIComponent(match[1])
      if (match[3]) {
        var version = decodeURIComponent(match[3])
        var atSequence = Math.floor(Number(match[5]))
        if (atSequence) {
          withSequence(atSequence)
        } else {
          sequence(DIRECTORY, ecb(internalError, withSequence))
        }
      } else {
        versions(DIRECTORY, name, function (error, versions) {
          if (error) {
            internalError()
          } else {
            if (versions === null) {
              notFound()
            } else {
              sendJSON(versions)
            }
          }
        })
      }
    } else {
      notFound()
    }
  } else if (pathname === '/memory') {
    sendJSON(process.memoryUsage())
  } else {
    notFound()
  }

  function withSequence (sequence) {
    tree(
      DIRECTORY, name, version, sequence,
      function (error, tree) {
        if (error) {
          internalError()
        } else {
          if (!tree) {
            notFound()
          } else {
            sendJSON({
              package: name,
              version: version,
              sequence: sequence,
              tree: tree
            })
          }
        }
      }
    )
  }

  function sendJSON (object) {
    response.statusCode = 200
    response.setHeader('Content-Type', 'application/json')
    response.end(JSON.stringify(object))
  }

  function internalError () {
    response.statusCode = 500
    response.end()
  }

  function notFound () {
    response.statusCode = 404
    response.end()
  }
})

server.listen(process.env.PORT || 8080, function () {
  log.info({port: this.address().port}, 'listening')
})

server.on('close', function () {
  log.info('close')
})
