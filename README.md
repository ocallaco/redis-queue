Redis-queue
-------

A system for organizing execution of jobs using redis and async.  Because of redis's support for lua scripting, there's no need for a sentinel process to coordinate workers.  Because the system uses async, workers don't need to poll the db -- they subscribe to redis channels that wake them whenever a new task is ready for execution.

Queue Types:

   Queue: simple FIFO queue.  If job is given a jobHash, it will be ignored
   
   LBQueue: balanced to allow jobs to have a priority.  Requires a jobHash, with optional priority.  When no priority is given, priority is determined by the number of jobs with identical jobHashes on the queue.  Otherwise, priority is the priority of the most recently enqueued job of the same priority.  If a job is enqueued while another worker is performing that job, it will be added to a waiting list and added to the queue again when that job completes (to allow for non-simultaneous running of identical jobs)

   DELQueue: a delayed queue that will run a job at a set time in the future.  When a job is enqueued without a timestamp, it is set to run immediately.  If identical jobs are enqueued with different timestamps, both jobs will run.  If identical jobs are enqueued with the same timestamp, only one will run.

Prerequisites:
----------

* Redis (http://redis.io)
* Torch (http://torch.ch)
* Luarocks

In Rockspec:

* Async
* Redis-async
* CJSON

Examples
--------

Initializing the environment for enqueueing:

```lua
local q = require 'redis-queue'
local rc = require 'redis-async'

local redis_queue
rc.connect({host='localhost', port=6379}, function(client)
   redis_client = client
   q(redis_client, function(newqueue)
   
      redis_queue = newqueue
   end)
end)

```

Initializing the environment for working:


```lua
local q = require 'redis-queue'
local rc = require 'redis-async'

local jobDescription = {
   QUEUENAME = {
      myJob1 = function(args)
         print(args)
      end
   }
}

local redis_queue
rc.connect({host='localhost', port=6379}, function(client)
   redis_client = client
   q(redis_client, function(newqueue)
      redis_queue = newqueue
      redis_queue:registerWorker({host='localhost', port=6379}, jobDescription)
   end)
end)

```

In this case, you need to have a hash in redis with the key "RESERVED:QCONFIG", matching "QUEUENAME" to a queue type for instance:

```
redis 127.0.0.1:6379> hset RESERVED:QCONFIG QUEUENAME QUEUE
(integer) 1
redis 127.0.0.1:6379> hset RESERVED:QCONFIG USER LBQUEUE
(integer) 1
redis 127.0.0.1:6379> hset RESERVED:QCONFIG TAG DELQUEUE
(integer) 1
redis 127.0.0.1:6379> hgetall RESERVED:QCONFIG
1) "QUEUENAME"
2) "QUEUE"
3) "USER"
4) "LBQUEUE"
5) "TAG"
6) "DELQUEUE"
```

Enqueuing a Regular job:
```lua
redis_queue:enqueueJob("QUEUENAME", "myJob1", {var1 = "test", var2 = 5, var3 = "another variable}, {jobHash = "test"}, callback)

```

the arguments are the name of the queue, the name of the job, the arguments for the job, additional args, and a callback.

additional args:
(* denotes required

QUEUE:  jobHash
LBQUEUE: *jobHash priority
DELQUEUE: *jobHash timestamp


License
-------

MIT License

