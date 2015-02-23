// Copyright (c) 2015 Uber Technologies, Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

'use strict';

var v2 = require('./v2');
var nullLogger = require('./null-logger.js');
var globalClearTimeout = require('timers').clearTimeout;
var globalSetTimeout = require('timers').setTimeout;
var globalNow = Date.now;
var globalRandom = Math.random;
var net = require('net');
var format = require('util').format;
var inspect = require('util').inspect;
var hexer = require('hexer');
var errSer = require('./lib/error_serial');

function TChannel(options) {
    if (!(this instanceof TChannel)) {
        return new TChannel(options);
    }

    var self = this;

    self.options = options || {};
    self.logger = self.options.logger || nullLogger;
    // TODO do not default the host.
    self.host = self.options.host || '127.0.0.1';
    // TODO do not default the port.
    self.port = self.options.port || 4040;
    self.hostPort = self.host + ':' + self.port;
    // TODO: maybe we should always add pid to user-supplied?
    self.processName = self.options.processName ||
        format('%s[%s]', process.title, process.pid);
    self.random = self.options.random ?
        self.options.random : globalRandom;
    self.setTimeout = self.options.timers ?
        self.options.timers.setTimeout : globalSetTimeout;
    self.clearTimeout = self.options.timers ?
        self.options.timers.clearTimeout : globalClearTimeout;
    self.now = self.options.timers ?
        self.options.timers.now : globalNow;

    self.reqTimeoutDefault = self.options.reqTimeoutDefault || 5000;
    self.serverTimeoutDefault = self.options.serverTimeoutDefault || 5000;
    self.timeoutCheckInterval = self.options.timeoutCheckInterval || 1000;
    self.timeoutFuzz = self.options.timeoutFuzz || 100;

    self.peers = Object.create(null);

    self.endpoints = Object.create(null);
    self.destroyed = false;
    // to provide backward compatibility.
    self.listening = self.options.listening === false ?
      false : true;
    self.serverSocket = new net.createServer(function onServerSocketConnection(sock) {
        if (!self.destroyed) {
            var remoteAddr = sock.remoteAddress + ':' + sock.remotePort;
            var conn = new TChannelConnection(self, sock, 'in', remoteAddr);
            self.logger.debug('incoming server connection', {
                hostPort: self.hostPort,
                remoteAddr: conn.remoteAddr
            });
        }
    });

    self.serverSocket.on('listening', function onServerSocketListening() {
        if (!self.destroyed) {
            self.logger.info(self.hostPort + ' listening');
            self.emit('listening');
        }
    });
    self.serverSocket.on('error', function onServerSocketError(err) {
        self.logger.error(self.hostPort + ' server socket error: ' + inspect(err));
    });
    self.serverSocket.on('close', function onServerSocketClose() {
        self.logger.warn('server socket close');
    });

    if (self.listening) {
        self.listen();
    }
}
require('util').inherits(TChannel, require('events').EventEmitter);

// Decoulping config and creation from the constructor.
// This also allows us to better unit test the code as the test process
// is not blocked by the listening connections
TChannel.prototype.listen = function listen() {
    var self = this;
    if (!self.serverSocket) {
        throw new Error('Missing server Socket.');
    }
    if (!self.host) {
        throw new Error('Missing server host.');
    }
    if (!self.port) {
        throw new Error('Missing server port.');
    }

    self.serverSocket.listen(self.port, self.host);
};


TChannel.prototype.register = function register(op, callback) {
    var self = this;
    self.endpoints[op] = callback;
};

TChannel.prototype.setPeer = function setPeer(hostPort, conn) {
    var self = this;
    if (hostPort === self.hostPort) {
        throw new Error('refusing to set self peer');
    }

    var list = self.peers[hostPort];
    if (!list) {
        list = self.peers[hostPort] = [];
    }

    if (conn.direction === 'out') {
        self.logger.debug('set out peer', {
            hostPort: self.hostPort,
            peerHostPort: hostPort,
            count: list.length
        });
        list.unshift(conn);
    } else {
        self.logger.debug('set in peer', {
            hostPort: hostPort,
            count: list.length
        });
        list.push(conn);
    }
    return conn;
};

TChannel.prototype.getPeer = function getPeer(hostPort) {
    var self = this;
    var list = self.peers[hostPort];
    var peer = list && list[0] ? list[0] : null;
    self.logger.debug('get peer attempt', {
        hostPort: self.hostPort,
        dest: hostPort,
        count: list ? list.length : 0,
        hasPeer: Boolean(peer)
    });
    return peer;
};

TChannel.prototype.removePeer = function removePeer(hostPort, conn) {
    var self = this;
    var list = self.peers[hostPort];
    var index = list ? list.indexOf(conn) : -1;

    if (index === -1) {
        return;
    }

    // TODO: run (don't walk) away from "arrays" as peers, get to actual peer
    // objects... note how these current semantics can implicitly convert
    // an in socket to an out socket
    list.splice(index, 1);
};

TChannel.prototype.getPeers = function getPeers() {
    var self = this;
    var keys = Object.keys(self.peers);

    var peers = [];
    for (var i = 0; i < keys.length; i++) {
        var list = self.peers[keys[i]];

        for (var j = 0; j < list.length; j++) {
            peers.push(list[j]);
        }
    }

    return peers;
};

TChannel.prototype.addPeer = function addPeer(hostPort, connection) {
    var self = this;
    if (hostPort === self.hostPort) {
        throw new Error('refusing to add self peer');
    }

    if (!connection) {
        connection = self.makeOutConnection(hostPort);
    }

    var existingPeer = self.getPeer(hostPort);
    if (existingPeer !== null && existingPeer !== connection) { // TODO: how about === undefined?
        self.logger.warn('allocated a connection twice', {
            hostPort: hostPort,
            direction: connection.direction
            // TODO: more log context
        });
    }

    self.logger.debug('alloc peer', {
        source: self.hostPort,
        destination: hostPort,
        direction: connection.direction,
        stack: (new Error()).stack.split(/\n/)
        // TODO: more log context
    });
    connection.once('reset', function onConnectionReset(/* err */) {
        // TODO: log?
        self.removePeer(hostPort, connection);
    });
    connection.once('socketClose', function onConnectionSocketClose(conn, err) {
        self.emit('socketClose', conn, err);
    });
    return self.setPeer(hostPort, connection);
};

/* jshint maxparams:5 */
TChannel.prototype.send = function send(options, arg1, arg2, arg3, callback) {
    var self = this;
    if (self.destroyed) {
        throw new Error('cannot send() to destroyed tchannel');
    }

    var dest = options.host;
    if (!dest) {
        throw new Error('cannot send() without options.host');
    }

    var reqBody = self.buildRequest(options, arg1, arg2, arg3);
    self.logger.debug('PRE get out conn');
    var peer = self.getOutConnection(dest);
    self.logger.debug('POST get out conn');
    peer.send(reqBody, callback);
    self.logger.debug('POST send');
};
/* jshint maxparams:4 */

TChannel.prototype.buildRequest = function buildRequest(options, arg1, arg2, arg3) {
    var ttl = options.timeout || 1; // TODO: better default, support for dynamic
    var tracing = options.tracing || null; // TODO: generate
    var service = options.service || null; // TODO: what even is this? default it to some sort of "my" name?
    var headers = options.headers || {}; // XXX
    var csum = v2.Checksum(v2.Checksum.Types.FarmHash32); // XXX
    var reqBody = v2.CallRequest(ttl, tracing, service, headers, arg1, arg2, arg3, csum);
    return reqBody;
};

TChannel.prototype.getOutConnection = function getOutConnection(dest) {
    var self = this;
    var peer = self.getPeer(dest);
    if (!peer) {
        peer = self.addPeer(dest);
    }
    return peer;
};

TChannel.prototype.makeSocket = function makeSocket(dest) {
    var parts = dest.split(':');
    if (parts.length !== 2) {
        throw new Error('invalid destination');
    }
    var host = parts[0];
    var port = parts[1];
    var socket = net.createConnection({host: host, port: port});
    return socket;
};

TChannel.prototype.makeOutConnection = function makeOutConnection(dest) {
    var self = this;
    var socket = self.makeSocket(dest);
    var connection = new TChannelConnection(self, socket, 'out', dest);
    return connection;
};

TChannel.prototype.quit = function quit(callback) {
    var self = this;
    self.destroyed = true;
    var peers = self.getPeers();
    var counter = peers.length + 1;

    self.logger.debug('quitting tchannel', {
        hostPort: self.hostPort
    });

    peers.forEach(function eachPeer(conn) {
        var sock = conn.socket;
        sock.once('close', onClose);

        conn.clearTimeoutTimer();

        self.logger.debug('destroy channel for', {
            direction: conn.direction,
            peerRemoteAddr: conn.remoteAddr,
            peerRemoteName: conn.remoteName,
            fromAddress: sock.address()
        });
        conn.closing = true;
        conn.resetAll(new Error('shutdown from quit'));
        sock.end();
    });

    var serverSocket = self.serverSocket;
    if (serverSocket.address()) {
        closeServerSocket();
    } else {
        serverSocket.once('listening', closeServerSocket);
    }

    function closeServerSocket() {
        serverSocket.once('close', onClose);
        serverSocket.close();
    }

    function onClose() {
        if (--counter <= 0) {
            if (counter < 0) {
                self.logger.error('closed more sockets than expected', {
                    counter: counter
                });
            }
            if (typeof callback === 'function') {
                callback();
            }
        }
    }
};

function TChannelConnection(channel, socket, direction, remoteAddr) {
    var self = this;
    if (remoteAddr === channel.hostPort) {
        throw new Error('refusing to create self connection');
    }

    self.channel = channel;
    self.logger = self.channel.logger;
    self.socket = socket;
    self.direction = direction;
    self.remoteAddr = remoteAddr;
    self.timer = null;

    self.remoteName = null; // filled in by identify message

    // TODO: factor out an operation collection abstraction
    self.inOps = Object.create(null);
    self.inPending = 0;
    self.outOps = Object.create(null);
    self.outPending = 0;

    self.lastSentFrameId = 0;
    self.lastTimeoutTime = 0;
    self.closing = false;

    self.parser = new v2.Parser(v2.Frame);

    self.socket.setNoDelay(true);

    self.socket.on('data', function onSocketData(chunk) {
        if (!self.closing) {
            self.parser.execute(chunk);
        }
    });
    self.socket.on('error', function onSocketError(err) {
        self.onSocketErr(err);
    });
    self.socket.on('close', function onSocketClose() {
        self.onSocketErr(new Error('socket closed'));
    });

    self.parser.on('frame', function onParserFrame(frame) {
        if (!self.closing) {
            self.onFrame(frame);
        }
    });
    self.parser.on('error', function onParserError(err) {
        if (!self.closing) {
            self.onParserError(err);
        }
    });

    if (direction === 'out') {
        self.sendInitRequest();
    }

    self.startTimeoutTimer();

    socket.once('close', clearTimer);

    function clearTimer() {
        self.channel.clearTimeout(self.timer);
    }
}
require('util').inherits(TChannelConnection, require('events').EventEmitter);

TChannelConnection.prototype.onParserError = function onParserError(err) {
    var self = this;
    var mess = 'tchannel parse error';
    if (err.buffer) {
        mess += '\n' + hexer(err.buffer);
        delete err.buffer;
    }
    self.channel.logger.error(mess, {
        remoteName: self.remoteName,
        localName: self.channel.hostPort,
        error: err
    });
    // TODO should we close the connection?
};

TChannelConnection.prototype.nextFrameId = function nextFrameId() {
    var self = this;
    self.lastSentFrameId = (self.lastSentFrameId + 1) % 0xffffffff;
    return self.lastSentFrameId;
};

// timeout check runs every timeoutCheckInterval +/- some random fuzz. Range is from
//   base - fuzz/2 to base + fuzz/2
TChannelConnection.prototype.getTimeoutDelay = function getTimeoutDelay() {
    var self = this;
    var base = self.channel.timeoutCheckInterval;
    var fuzz = self.channel.timeoutFuzz;
    return base + Math.round(Math.floor(self.channel.random() * fuzz) - (fuzz / 2));
};

TChannelConnection.prototype.startTimeoutTimer = function startTimeoutTimer() {
    var self = this;
    self.timer = self.channel.setTimeout(function onChannelTimeout() {
        // TODO: worth it to clear the fired self.timer objcet?
        self.onTimeoutCheck();
    }, self.getTimeoutDelay());
};

TChannelConnection.prototype.clearTimeoutTimer = function clearTimeoutTimer() {
    var self = this;
    if (self.timer) {
        self.channel.clearTimeout(self.timer);
        self.timer = null;
    }
};

// If the connection has some success and some timeouts, we should probably leave it up,
// but if everything is timing out, then we should kill the connection.
TChannelConnection.prototype.onTimeoutCheck = function onTimeoutCheck() {
    var self = this;
    if (self.closing) {
        return;
    }

    if (self.lastTimeoutTime) {
        self.logger.warn(self.channel.hostPort + ' destroying socket from timeouts');
        self.socket.destroy();
        return;
    }

    self.checkOutOpsForTimeout(self.outOps);
    self.checkInOpsForTimeout(self.inOps);

    self.startTimeoutTimer();
};

TChannelConnection.prototype.checkInOpsForTimeout = function checkInOpsForTimeout(ops) {
    var self = this;
    var opKeys = Object.keys(ops);
    var now = self.channel.now();

    for (var i = 0; i < opKeys.length; i++) {
        var opKey = opKeys[i];
        var op = ops[opKey];

        if (op === undefined) {
            continue;
        }

        var timeout = self.channel.serverTimeoutDefault;
        var duration = now - op.start;
        if (duration > timeout) {
            delete ops[opKey];
            self.inPending--;
        }
    }
};

TChannelConnection.prototype.checkOutOpsForTimeout = function checkOutOpsForTimeout(ops) {
    var self = this;
    var opKeys = Object.keys(ops);
    var now = self.channel.now();
    for (var i = 0; i < opKeys.length ; i++) {
        var opKey = opKeys[i];
        var op = ops[opKey];
        if (op.timedOut) {
            delete ops[opKey];
            self.outPending--;
            self.logger.warn('lingering timed-out outgoing operation');
            continue;
        }
        if (op === undefined) {
            // TODO: why not null and empty string too? I mean I guess false
            // and 0 might be a thing, but really why not just !op?
            self.channel.logger
                .warn('unexpected undefined operation', {
                    key: opKey,
                    op: op
                });
            continue;
        }
        var timeout = op.timeout || self.channel.reqTimeoutDefault;
        var duration = now - op.start;
        if (duration > timeout) {
            delete ops[opKey];
            self.outPending--;
            self.onReqTimeout(op);
        }
    }
};

TChannelConnection.prototype.onReqTimeout = function onReqTimeout(op) {
    var self = this;
    op.timedOut = true;
    op.callback(new Error('timed out'), null, null);
    self.lastTimeoutTime = self.channel.now();
};

// this socket is completely broken, and is going away
// In addition to erroring out all of the pending work, we reset the state in case anybody
// stumbles across this object in a core dump.
TChannelConnection.prototype.resetAll = function resetAll(err) {
    var self = this;
    if (self.closing) return;
    self.closing = true;

    var inOpKeys = Object.keys(self.inOps);
    var outOpKeys = Object.keys(self.outOps);

    self.logger[err ? 'warn' : 'info']('resetting connection', {
        error: err,
        remoteName: self.remoteName,
        localName: self.channel.hostPort,
        numInOps: inOpKeys.length,
        numOutOps: outOpKeys.length,
        inPending: self.inPending,
        outPending: self.outPending
    });

    self.clearTimeoutTimer();

    self.emit('reset');

    // requests that we've received we can delete, but these reqs may have started their
    //   own outgoing work, which is hard to cancel. By setting this.closing, we make sure
    //   that once they do finish that their callback will swallow the response.
    inOpKeys.forEach(function eachInOp(id) {
        // TODO: we could support an op.cancel opt-in callback
        delete self.inOps[id];
    });

    // for all outgoing requests, forward the triggering error to the user callback
    outOpKeys.forEach(function eachOutOp(id) {
        var op = self.outOps[id];
        delete self.outOps[id];
        // TODO: shared mutable object... use Object.create(err)?
        op.callback(err, null, null);
    });

    self.inPending = 0;
    self.outPending = 0;

    self.emit('socketClose', self, err);
};

TChannelConnection.prototype.onSocketErr = function onSocketErr(err) {
    var self = this;
    if (!self.closing) {
        self.resetAll(err);
    }
};

TChannelConnection.prototype.onFrame = function onFrame(frame) {
    var self = this;
    self.logger.debug('onFrame', {
        hostPort: self.channel.hostPort,
        dest: self.remoteAddr,
        remoteName: self.remoteName,
        direction: self.direction,
        frame: frame
    });
    self.lastTimeoutTime = 0;
    switch (frame.body.type) {
        case v2.Types.InitRequest:
            return self.handleInitRequest(frame);
        case v2.Types.InitResponse:
            return self.handleInitResponse(frame);
        case v2.Types.CallRequest:
            return self.handleCallRequest(frame);
        case v2.Types.CallResponse:
            return self.handleCallResponse(frame);
        case v2.Types.Error:
            return self.handleError(frame);
        default:
            self.logger.error('unhandled frame type', {
                type: frame.body.type
            });
    }
};

TChannelConnection.prototype.handleCallRequest = function handleCallRequest(reqFrame) {
    var self = this;
    if (self.remoteName === null) {
        self.resetAll(new Error('call request before init request')); // TODO typed error
        return;
    }

    var id = reqFrame.id;
    var name = reqFrame.body.arg1;
    var handler = self.channel.endpoints[name];
    self.logger.debug('handleReqFrame', {
        hostPort: self.channel.hostPort,
        opId: id,
        arg1: name
    });

    if (typeof handler !== 'function') {
        // TODO: test this behavior, in fact the prior early return subtlety
        // broke tests in an unknown way after deferring the inOps mutation
        // until after old handler verification without this... arguably it's
        // what we want anyhow, but that weird test failure should be
        // understood
        handler = function noSuchHandler(arg2, arg3, remoteAddr, cb) {
            var err = new Error('no such operation');
            err.op = name;
            cb(err, null, null);
        };

        self.channel.emit('endpoint.missing', {
            name: name
        });

        return;
    } else {
        self.channel.emit('endpoint', {
            name: name
        });
    }

    // TODO: couple reqFrame.body.tracing for all downstream requests
    // TODO: implement service name?

    self.inPending++;
    var op = self.inOps[id] = new TChannelServerOp(self,
        handler, reqFrame, self.channel.now(), sendFrame);

    function sendFrame(resFrame) {
        if (self.inOps[id] !== op) {
            self.logger.warn('attempt to send frame for mismatched operation', {
                hostPort: self.channel.hostPort,
                opId: id
            });
            return;
        }
        delete self.inOps[id];
        self.inPending--;
        if (!self.closing) {
            // TODO: should wrap in a try/catch... or add an error channel to write
            var buf = resFrame.toBuffer();
            self.socket.write(buf);
        }
        self.logger.debug('sendFrame', {
            hostPort: self.channel.hostPort,
            closing: self.closing,
            opId: id
        });
    }
};

TChannelConnection.prototype.handleCallResponse = function handleCallResponse(resFrame) {
    var self = this;
    if (self.remoteName === null) {
        self.resetAll(new Error('call response before init response')); // TODO typed error
        return;
    }
    var id = resFrame.id;
    var code = resFrame.body.code;
    var arg1 = resFrame.body.arg1;
    var arg2 = resFrame.body.arg2;
    var arg3 = resFrame.body.arg3;
    var err = self.errorForCode(code, arg1);
    self.completeOutOp(id, err, arg2, arg3);
};

TChannelConnection.prototype.handleError = function handleError(errFrame) {
    var self = this;
    var id = errFrame.id;
    var code = errFrame.body.code;
    var message = errFrame.body.message;
    var err = self.errorForCode(code, message);
    self.completeOutOp(id, err, null, null);
};

TChannelConnection.prototype.errorForCode = function errorForCode(code, message) {
    if (code === v2.CallResponse.Codes.OK) {
        return null;
    }

    var passedError = errSer.deserialize(message);
    if (code === v2.CallResponse.Codes.AppException) {
        return passedError;
    }

    var err;
    switch (code) {
        case v2.CallResponse.Codes.Timeout:
            err = new Error('tchannel timeout'); // TODO typed error
            break;
        case v2.CallResponse.Codes.Cancelled:
            err = new Error('tchannel canceled'); // TODO typed error
            break;
        case v2.CallResponse.Codes.Busy:
            err = new Error('tchannel busy'); // TODO typed error
            break;
        case v2.CallResponse.Codes.SocketErrorNoRetries:
            err = new Error('tchannel socket error, no retries'); // TODO typed error
            break;
        case v2.CallResponse.Codes.SocketError:
            err = new Error('tchannel socket error, retries exhausted'); // TODO typed error
            break;
        default:
            err = new Error('tchannel invalid response code ' + code); // TODO typed error
    }
    err.passedError = passedError;
    return err;
};

TChannelConnection.prototype.completeOutOp = function completeOutOp(id, err, arg1, arg2) {
    var self = this;
    var op = self.outOps[id];
    if (!op) {
        // TODO else case. We should warn about an incoming response for an
        // operation we did not send out.  This could be because of a timeout
        // or could be because of a confused / corrupted server.
        return;
    }
    delete self.outOps[id];
    self.outPending--;
    op.callback(err, arg1, arg2);
};

TChannelConnection.prototype.sendInitRequest = function sendInitRequest() {
    var self = this;
    var id = self.nextFrameId(); // TODO: assert(id === 1)?
    var body = v2.InitRequest(v2.VERSION, self.channel.hostPort, self.channel.processName);
    var frame = v2.Frame(id, 0, body);
    var buffer = frame.toBuffer();
    self.logger.debug('sending init request', {
        hostPort: self.channel.hostPort,
        remoteAddr: self.remoteAddr
    });
    self.socket.write(buffer);
};

TChannelConnection.prototype.sendInitResponse = function sendInitResponse() {
    var self = this;
    var id = self.nextFrameId(); // TODO: assert(id === 1)?
    var body = v2.InitResponse(v2.VERSION, self.channel.hostPort, self.channel.processName);
    var frame = v2.Frame(id, 0, body);
    var buffer = frame.toBuffer();
    self.logger.debug('sending init response', {
        hostPort: self.channel.hostPort,
        remoteAddr: self.remoteAddr
    });
    self.socket.write(buffer);
};

TChannelConnection.prototype.handleInitRequest = function handleInitRequest(reqFrame) {
    var self = this;
    if (self.remoteName !== null) {
        self.resetAll(new Error('duplicate init request')); // TODO typed error
        return;
    }
    var hostPort = reqFrame.body.hostPort;
    var processName = reqFrame.body.processName;
    self.remoteName = hostPort;
    // TODO: use processName
    self.channel.addPeer(hostPort, self);
    self.logger.debug('got init request', {
        hostPort: self.channel.hostPort,
        remoteHostPort: hostPort
    });
    self.channel.emit('identified', hostPort, processName);
    self.sendInitResponse();
};

TChannelConnection.prototype.handleInitResponse = function handleInitResponse(resFrame) {
    var self = this;
    if (self.remoteName !== null) {
        self.resetAll(new Error('duplicate init response')); // TODO typed error
        return;
    }
    var hostPort = resFrame.body.hostPort;
    var processName = resFrame.body.processName;
    // TODO: use processName
    self.remoteName = hostPort;
    self.logger.debug('got init response', {
        hostPort: self.channel.hostPort,
        remoteHostPort: hostPort
    });
    self.channel.emit('identified', hostPort, processName);
};

// send a req frame
/* jshint maxparams:5 */
TChannelConnection.prototype.send = function send(reqBody, callback) {
    var self = this;
    var id = self.nextFrameId();
    // TODO: use this to protect against >4Mi outstanding messages edge case
    // (e.g. zombie operation bug, incredible throughput, or simply very long
    // timeout
    // if (self.outOps[id]) {
    //  throw new Error('duplicate frame id in flight');
    // }

    var reqFrame = v2.Frame(id, 0, reqBody);
    self.outOps[id] = new TChannelClientOp(
        reqFrame, self.channel.now(), callback);
    self.pendingCount++;
    var buffer = reqFrame.toBuffer();
    return self.socket.write(buffer);
};
/* jshint maxparams:4 */

/* jshint maxparams:6 */
function TChannelServerOp(connection, handler, reqFrame, start, sendFrame) {
    var self = this;
    self.connection = connection;
    self.logger = connection.logger;
    self.handler = handler;
    self.reqFrame = reqFrame;
    self.timedOut = false;
    self.timeout = reqFrame.body.ttl; // TODO: provide defalut?
    self.start = start;
    self.headers = {};
    self.checksumType = reqFrame.body.csum.type;
    self.sendFrame = sendFrame;
    self.responseSent = false;
    process.nextTick(function runHandler() {
        self.handler(reqFrame.body.arg2, reqFrame.body.arg3, connection.remoteName, sendResponse);
    });
    function sendResponse(err, res1, res2) {
        self.sendResponse(err, res1, res2);
    }
}
/* jshint maxparams:4 */

TChannelServerOp.prototype.sendResponse = function sendResponse(err, res1, res2) {
    var self = this;
    if (self.responseSent) {
        self.logger.error('response already sent', {
            err: err,
            res1: res1,
            res2: res2
        });
        return;
    }
    self.responseSent = true;
    // TODO: observability hook for handler errors
    var resFrame = self.buildResponseFrame(err, res1, res2);
    self.sendFrame(resFrame);
};

TChannelServerOp.prototype.buildResponseFrame = function buildResponseFrame(err, res1, res2) {
    var self = this;
    var id = self.reqFrame.id;
    var code = v2.CallResponse.Codes.OK;
    var csum = v2.Checksum(self.checksumType);
    var arg1 = self.reqFrame.body.arg1;
    if (err) {
        code = v2.CallResponse.Codes.AppException;
        arg1 = errSer.serialize(err);
    }
    var resBody = v2.CallResponse(code, self.headers, arg1, res1, res2, csum);
    var resFrame = v2.Frame(id, 0, resBody);
    return resFrame;
};

function TChannelClientOp(frame, start, callback) {
    var self = this;
    self.frame = frame;
    self.callback = callback;
    self.start = start;
    self.timeout = frame.body.ttl; // TODO: provide defalut?
    self.timedOut = false;
}

module.exports = TChannel;
