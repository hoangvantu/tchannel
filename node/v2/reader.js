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

var TypedError = require('error/typed');
var inherits = require('util').inherits;
var Transform = require('stream').Transform;
var ParseBuffer = require('./parse_buffer');
var bufrw = require('bufrw');

var ZeroLengthFrameError = TypedError({
    type: 'tchannel.zero-length-frame',
    message: 'zero length frame encountered'
});

var BrokenReaderStateError = TypedError({
    type: 'tchannel.broken-reader-state',
    message: 'reader in invalid state {state} expecting {expecting} avail {aval}',
    state: null,
    expecting: null,
    avail: null
});

var TruncatedReadError = TypedError({
    type: 'tchannel.truncated-read',
    message: 'read truncated by end of stream with {length} bytes in buffer',
    length: null,
    buffer: null,
    state: null,
    expecting: null
});

module.exports = ChunkReader;

var States = {
    PendingLength: 0,
    Seeking: 1
};

function ChunkReader(FrameType, options) {
    if (!(this instanceof ChunkReader)) {
        return new ChunkReader(FrameType, options);
    }
    options = options || {};
    Transform.call(this, options);
    var self = this;
    self._readableState.objectMode = true;
    self.FrameType = FrameType;
    self.buffer = new ParseBuffer();
    self.frameLengthSize = options.frameLengthSize || 2;
    self.expecting = self.frameLengthSize;
    self.state = States.PendingLength;
    switch (self.frameLengthSize) {
        case 1:
            self._readLength = self._readUInt8Length;
            break;
        case 2:
            self._readLength = self._readUInt16BELength;
            break;
        case 4:
            self._readLength = self._readUInt32BELength;
            break;
        default:
            throw new Error('unsupported frame length size');
    }
}

inherits(ChunkReader, Transform);

ChunkReader.prototype._transform = function _transform(chunk, encoding, callback) {
    var self = this;
    if (!callback) {
        callback = emitIt;
    }
    self.buffer.push(chunk);
    while (self.buffer.avail() >= self.expecting) {
        switch (self.state) {
            case States.PendingLength:
                self.expecting = self._readLength();
                if (!self.expecting) {
                    self.emit('error', ZeroLengthFrameError());
                    self.buffer.shift(self.frameLengthSize);
                    self.expecting = self.frameLengthSize;
                    self.state = States.PendingLength;
                } else {
                    self.state = States.Seeking;
                }
                break;
            case States.Seeking:
                var frameChunk = self.buffer.shift(self.expecting);
                if (!frameChunk.length) {
                    callback(BrokenReaderStateError({
                        state: self.state,
                        expecting: self.expecting,
                        avail: self.buffer.avail()
                    }));
                    return;
                }
                self.handleFrame(frameChunk);
                self.expecting = self.frameLengthSize;
                self.state = States.PendingLength;
                break;
            default:
                callback(BrokenReaderStateError({
                    state: self.state,
                    expecting: self.expecting,
                    avail: self.buffer.avail()
                }));
                return;
        }
    }
    callback();

    function emitIt(err) {
        self.emit('error', err);
    }
};

ChunkReader.prototype._flush = function _flush(callback) {
    var self = this;
    var avail = self.buffer.avail();
    if (avail) {
        callback(TruncatedReadError({
            length: avail,
            state: self.state,
            expecting: self.expecting
        }));
        self.buffer.clear();
        self.expecting = 4;
        self.state = States.PendingLength;
    } else {
        callback();
    }
};

ChunkReader.prototype.handleFrame = function handleFrame(chunk, callback) {
    var self = this;
    if (!callback) {
        callback = emitFrame;
    }
    var tup = bufrw.fromBufferTuple(self.FrameType.struct, chunk);
    var err = tup[0];
    var frame = tup[1];
    if (err) {
        callback(err, frame);
    } else {
        callback(null, frame);
    }

    function emitFrame(err, frame) {
        if (err) {
            self.emit('error', err);
        } else {
            self.push(frame);
        }
    }
};

ChunkReader.prototype._readUInt8Length = function _readUInt8Length() {
    var self = this;
    return self.buffer.readUInt8(0);
};

ChunkReader.prototype._readUInt16BELength = function _readUInt16BELength() {
    var self = this;
    return self.buffer.readUInt16BE(0);
};

ChunkReader.prototype._readUInt32BELength = function _readUInt32BELength() {
    var self = this;
    return self.buffer.readUInt32BE(0);
};
