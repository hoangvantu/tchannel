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
var bufrw = require('bufrw');
var header = require('./header');

module.exports.Request = InitRequest;
module.exports.Response = InitResponse;

var MissingInitHeaderError = TypedError({
    type: 'tchannel.missing-init-header',
    message: 'missing init frame header {field}',
    field: null
});

var RequiredHeaderFields = ['host_port', 'process_name'];

function InitRequest(version, headers) {
    if (!(this instanceof InitRequest)) {
        return new InitRequest(version, headers);
    }
    var self = this;
    self.type = InitRequest.TypeCode;
    self.version = version || 0;
    self.headers = headers || {};
}

InitRequest.TypeCode = 0x01;

function requiredFieldGuard(headers) {
    for (var i = 0; i < RequiredHeaderFields.length; i++) {
        var field = RequiredHeaderFields[i];
        if (headers[field] === undefined) {
            return MissingInitHeaderError({field: field});
        }
    }
    return null;
}

InitRequest.struct = bufrw.Struct(InitRequest, {
    version: bufrw.UInt16BE, // version:2
    headers: header.header2  // nh:2 (hk~2 hv~2){nh}
}, {
    writeGuard: requiredFieldGuard,
    readGuard: requiredFieldGuard
});

// TODO: MissingInitHeaderError check / guard

function InitResponse(version, headers) {
    if (!(this instanceof InitResponse)) {
        return new InitResponse(version, headers);
    }
    var self = this;
    self.type = InitResponse.TypeCode;
    self.version = version || 0;
    self.headers = headers || {};
}

InitResponse.TypeCode = 0x02;

InitResponse.struct = bufrw.Struct(InitResponse, {
    version: bufrw.UInt16BE, // version:2
    headers: header.header2  // nh:2 (hk~2 hv~2){nh}
});
