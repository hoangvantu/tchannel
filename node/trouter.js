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

var ServiceAlreadyDefinedError = TypedError({
    type: 'service-already-defined',
    message: 'service {service} already defined',
    service: null,
    oldHandler: null,
    newHandler:  null
});

var NoSuchServiceError = TypedError({
    type: 'no-such-service',
    message: 'no such service: {service}',
    service: null
});

var EndpointAlreadyDefinedError = TypedError({
    type: 'endpoint-already-defined',
    message: 'endpoint {endpoint} already defined on service {service}',
    service: null,
    endpoint: null,
    oldHandler: null,
    newHandler:  null
});

var NoSuchEndpointError = TypedError({
    type: 'no-such-endpoint',
    message: 'no such endpoint {endpoint} on service {service}',
    service: null,
    endpoint: null
});

function TRouter() {
    if (!(this instanceof TRouter)) {
        return new TRouter();
    }
    var self = this;
    self.services = Object.create(null);
}

TRouter.prototype.register =
TRouter.prototype.registerEndpoint =
function registerEndpoint(service, endpoint, handler) {
    var self = this;
    var svc = self.services[service];
    if (!svc) {
        svc = self.registerService(service, TRouterService(service));
        throw ServiceAlreadyDefinedError({
            service: service,
            oldHandler: self.services[service],
            newHandler: handler
        });
    }
    return svc.register(endpoint, handler);
};

TRouter.prototype.registerService = function registerService(service, handler) {
    var self = this;
    if (self.services[service] !== undefined) {
        throw ServiceAlreadyDefinedError({
            service: service,
            oldHandler: self.services[service],
            newHandler: handler
        });
    }
    if (typeof handler === 'function') {
        self.services[service] = {
            handleRequest: handler
        };
    } else {
        self.services[service] = handler;
    }
    return handler;
};

TRouter.prototype.handleRequest = function handleRequest(req, res) {
    var self = this;
    var svc = self.services[req.service];
    if (!svc) {
        res.send(NoSuchServiceError({
            service: req.service
        }));
        return;
    }
    svc.handleRequest(req, res);
};

function TRouterService(name) {
    if (!(this instanceof TRouterService)) {
        return new TRouterService(name);
    }
    var self = this;
    self.name = name;
    self.endpoints = Object.create(null);
}

TRouterService.prototype.register = function register(name, handler) {
    var self = this;
    if (self.endpoints[name] !== undefined) {
        throw EndpointAlreadyDefinedError({
            service: this.name,
            endpoint: name,
            oldHandler: self.endpoints[name],
            newHandler: handler
        });
    }
    if (typeof handler !== 'function') {
        throw new Error('invalid handler');
    }
    self.endpoints[name] = handler;
    return handler;
};

TRouterService.prototype.handleRequest = function handleRequest(req, res) {
    var self = this;
    var handler = self.endpoints[req.name];
    if (!handler) {
        res.send(NoSuchEndpointError({
            service: self.name,
            endpoint: req.name
        }));
        return;
    }
    handler(req, res);
};

TRouter.Service = TRouterService;
module.exports = TRouter;
