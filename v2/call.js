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

var read = require('../lib/read');
var write = require('../lib/write');
var Checksum = require('./checksum');
var header = require('./header');

module.exports.Request = CallRequest;
module.exports.Response = CallResponse;

var emptyBuffer = new Buffer(0);
var emptyTracing = new Buffer(24);
emptyTracing.fill(0);

/* jshint maxparams:8 */

function CallRequest(ttl, tracing, service, headers, arg1, arg2, arg3, csum) {
    if (!(this instanceof CallRequest)) {
        return new CallRequest(ttl, tracing, service, headers, arg1, arg2, arg3, csum);
    }
    var self = this;
    self.type = CallRequest.TypeCode;
    self.ttl = ttl;
    self.tracing = tracing || emptyTracing;
    self.service = service || emptyBuffer;
    self.headers = headers;
    self.arg1 = arg1 || emptyBuffer;
    self.arg2 = arg2 || emptyBuffer;
    self.arg3 = arg3 || emptyBuffer;
    self.csum = csum;
}

CallRequest.TypeCode = 0x03;

// ttl:4 tracing:24 service~2 nh:1 (hk~1 hv~1){nh} arg1~2 arg2~2 arg3~2 csumtype:1 (csum:4){0,1}
CallRequest.read = read.chained(read.series([
    read.UInt32BE,  // ttl:4
    read.fixed(24), // tracing:24
    read.buf2,      // service~2
    header.read,    // nh:1 (hk~1 hv~1){nh}
    read.buf2,      // arg1~2
    read.buf2,      // arg2~2
    read.buf2,      // arg3~2
    Checksum.read   // csumtype:1 (csum:4){0,1}
]), function buildCallReq(results, buffer, offset) {
    var ttl = results[0];
    var tracing = results[1];
    var service = results[2];
    var headers = results[3];
    var arg1 = results[4];
    var arg2 = results[5];
    var arg3 = results[6];
    var csum = results[7];
    var err = csum.verify(arg1, arg2, arg3);
    if (err) return [err, offset, null];
    var req = new CallRequest(ttl, tracing, service, headers, arg1, arg2, arg3, csum);
    return [null, offset, req];
});

// ttl:4 tracing:24 service~2 nh:1 (hk~1 hv~1){nh} arg1~2 arg2~2 arg3~2 csumtype:1 (csum:4){0,1}
CallRequest.prototype.write = function writeCallReq() {
    var self = this;
    var arg1 = write.bufferOrString(self.arg1, 'CallRequest arg1');
    var arg2 = write.bufferOrString(self.arg2, 'CallRequest arg2');
    var arg3 = write.bufferOrString(self.arg3, 'CallRequest arg3');
    self.csum.update(arg1, arg2, arg3);
    return write.series([
        write.UInt32BE(self.ttl, 'CallRequest ttl'),          // ttl:4
        write.fixed(24, self.tracing, 'CallRequest tracing'), // tracing:24
        write.buf2(self.service, 'CallRequest service'),      // service~2
        header.write(self.headers),                           // nh:1 (hk~1 hv~1){nh}
        write.buf2(arg1, 'CallRequest arg1'),                 // arg1~2
        write.buf2(arg2, 'CallRequest arg2'),                 // arg2~2
        write.buf2(arg3, 'CallRequest arg3'),                 // arg3~2
        self.csum.write()                                     // csumtype:1 (csum:4){0,1}
    ]);
};

function CallResponse(code, headers, arg1, arg2, arg3, csum) {
    if (!(this instanceof CallResponse)) {
        return new CallResponse(code, headers, arg1, arg2, arg3, csum);
    }
    var self = this;
    self.type = CallResponse.TypeCode;
    self.code = code;
    self.headers = headers;
    self.arg1 = arg1 || emptyBuffer;
    self.arg2 = arg2 || emptyBuffer;
    self.arg3 = arg3 || emptyBuffer;
    self.csum = csum;
}

CallResponse.TypeCode = 0x04;

CallResponse.Codes = {
    OK: 0x00,
    Timeout: 0x01,
    Cancelled: 0x02,
    Busy: 0x03,
    SocketErrorNoRetries: 0x04,
    SocketError: 0x05,
    AppException: 0x06
};

// code:1 nh:1 (hk~1 hv~1){nh} arg1~2 arg2~2 arg3~2 csumtype:1 (csum:4){0,1}
CallResponse.read = read.chained(read.series([
    read.UInt8,   // code:1
    header.read,  // nh:1 (hk~1 hv~1){nh}
    read.buf2,    // arg1~2
    read.buf2,    // arg2~2
    read.buf2,    // arg3~2
    Checksum.read // csumtype:1 (csum:4){0,1}
]), function buildCallRes(results, buffer, offset) {
    var code = results[0];
    var headers = results[1];
    var arg1 = results[2];
    var arg2 = results[3];
    var arg3 = results[4];
    var csum = results[5];
    var err = csum.verify(arg1, arg2, arg3);
    if (err) return [err, offset, null];
    var res = new CallResponse(code, headers, arg1, arg2, arg3, csum);
    return [null, offset, res];
});

// code:1 nh:1 (hk~1 hv~1){nh} arg1~2 arg2~2 arg3~2 csumtype:1 (csum:4){0,1}
CallResponse.prototype.write = function writeCallRes() {
    var self = this;
    var arg1 = write.bufferOrString(self.arg1, 'CallResponse arg1');
    var arg2 = write.bufferOrString(self.arg2, 'CallResponse arg2');
    var arg3 = write.bufferOrString(self.arg3, 'CallResponse arg3');
    self.csum.update(arg1, arg2, arg3);
    return write.series([
        write.UInt8(self.code, 'CallResponse code'), // code:1
        header.write(self.headers),                  // nh:1 (hk~1 hv~1){nh}
        write.buf2(arg1, 'CallResponse arg1'),       // arg1~2
        write.buf2(arg2, 'CallResponse arg2'),       // arg2~2
        write.buf2(arg3, 'CallResponse arg3'),       // arg3~2
        self.csum.write()                            // csumtype:1 (csum:4){0,1}
    ]);
};
