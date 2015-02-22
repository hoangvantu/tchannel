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

var extend = require('xtend/mutable');
var safeParse = require("safe-json-parse/tuple");
var stringify = require('json-stringify-safe');

module.exports.serialize = serializeError;
module.exports.deserialize = deserializeError;

function serializeError(err) {
    if (typeof err === 'string') {
        return err;
    } else {
        var data = null;
        if (isError(err)) {
            data = {$jsError: serializeErrorObject(err)};
        } else {
            data = err;
        }
        return stringify(data);
    }
}

function serializeErrorObject(err) {
    var data = {
        name: err.constructor.name,
        message: err.message,
        stack: err.stack
    };
    for (var prop in err) {
        if (err.hasOwnProperty(prop)) {
            data[prop] = err[prop];
        }
    }
    return data;
}

function deserializeError(str) {
    var tup = safeParse(str);
    if (tup[0]) {
        return str;
    }
    var data = tup[1];
    var ks = Object.keys(data);
    if (ks.length === 1 && ks[0] === '$jsError') {
        var name = data.name;
        if (!name) return data;
        var err = new Error(data.message);
        delete data.name;
        delete data.message;
        err = extend(err, data);
        return err;
    } else {
        return data;
    }
}

function isError(obj) {
    return typeof obj === 'object' && (
        Object.prototype.toString.call(obj) === '[object Error]' ||
        obj instanceof Error);
}
