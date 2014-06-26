Redis-queue
-------

A system for organizing execution of jobs using redis and async (https://github.com/clementfarabet/async).

Queues are managed through lua scripts executed on the redis, with workers subscribing to pub/sub channels to wake them up when new work is available.

Queue Types:

*Queue:* 
* simple FIFO queue.  
* If job is given a jobHash, it will overwrite any job of the same hash.
   
*LBQueue:* 
* Balanced to allow jobs to have a user-defined priority.  
* Requires a jobHash, with optional priority. 
* Jobs with identical hashes will never be executed simultaneously.
* If not running, will overwrite existing job on queue with new priority
* If running, will wait until execution of existing job completes before being added to queue
* When no priority is given, priority is incremented (multiple enqueues of the same job will move it up the queue)

*DELQueue:*
* A delayed queue that will run a job at a set time in the future.  
* Requires a jobHash
* When a job is enqueued without a timestamp, it is set to run immediately.  
* If jobs with the same hash are enqueued with different timestamps, both jobs will run at their chosen times. 
* If jobs with the same hash are enqueued with the same timestamp, only one will run.

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
redis_queue:enqueueJob("QUEUENAME", "myJob1", {var1 = "test", var2 = 5, var3 = "another variable"}, {jobHash = "test"}, callback)

```

the arguments are the name of the queue, the name of the job, the arguments for the job, additional args, and a callback.

additional args:
(* denotes required)

QUEUE:  jobHash


LBQUEUE: *jobHash priority


DELQUEUE: *jobHash timestamp


License
-------

MIT License

