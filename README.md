#FAQ

[![Build Status](https://travis-ci.org/criteo/memcache-driver.png?branch=master)](https://travis-ci.org/criteo/memcache-driver)
[![NuGet version](https://badge.fury.io/nu/Criteo.Memcache.png)](http://badge.fury.io/nu/Criteo.Memcache)

##What is our priority?
This driver's main purpose is to access memcached with high performance.
To that purpose, this driver multiplexes requests and is fully asynchronous.

##Is there a CLA?
No

##What are the features we want to add?
* Support of more memcache requests (see https://code.google.com/p/memcached/wiki/BinaryProtocolRevamped)
* Very simple UDP transport

##What we do not want to support
* Multi-get: multi-get as it's implemented in other binary memcache drivers is just a way to multiplex requests to reach better performance. This driver multiplexes all the requests, so there is no use to add a multi-get.
* Silent requests: We don’t want silent requests because it messes the underlying multiplexing. If you know how to do it you’re welcome but be aware it’s a complex topic.(GetQ, SetQ, AddQ, etc.)

##How to contribute
* Fork -> rebase on master -> pull request
* We'll review all changes before merging them, we ask for:
 * Performance
 * Readable code
 * Tests
 * In case of a bug fix, please try to also provide a test
 * Features aligned with our roadmap

##What if you don't fit with these requirements
* The API is plugin oriented, you can implement most of the main components and inject them with the factories.
* Fork the project, Github is also made for this.

#License

Licensed under the Apache 2.0 license.

#Copyright

Copyright © Criteo, 2014.
