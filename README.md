Redis-queue
-------

A system for organizing execution of jobs using redis and async.  Because of redis's support for lua scripting, there's no need for a sentinel process to coordinate workers.  Because the system uses async, workers don't need to poll the db -- they subscribe to redis channels that wake them whenever a new task is ready for execution.

License
-------

MIT License


