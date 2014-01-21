local common = require 'redis-queue.common'

local regularQueue = {}

local QUEUE = "QUEUE:"
local CHANNEL = "CHANNEL:"
local UNIQUE = "UNIQUE:"

local json = require 'cjson'

local evals = {
      -- enqueue on a standard queue -- check for hash uniqueness so we don't put the same job on twice
   -- note:  hsetnx() ALWAYS returns integer 1 or 0
   enqueue = function(queue, jobJson, jobName, jobHash, cb)

      local script = [[
      local job = ARGV[1]
      local jobName = ARGV[2]
      local jobHash = ARGV[3]

      local queue = KEYS[1]
      local chann = KEYS[2]
      local uniqueness = KEYS[3]

      local newjob = 0

      if jobHash and jobHash ~= "0" then
         newjob = redis.call('hsetnx', uniqueness, jobHash, 1)
      else
         newjob = 1
      end

      if newjob ~= 0 then
         redis.call('lpush', queue, job)
         redis.call('publish', chann, jobName)
      end
      ]] 
      return script, 3, QUEUE .. queue, CHANNEL .. queue, UNIQUE .. queue, jobJson, jobName, jobHash, cb
   end,

   reenqueue = function(queue, jobJson, jobName, jobHash, failureHash, cb)
-- script is the same as enqueue, just deletes the 
      local script = [[

      local job = ARGV[1]
      local jobName = ARGV[2]
      local failureHash = ARGV[3]
      local jobHash = ARGV[4]

      local queue = KEYS[1]
      local chann = KEYS[2]
      local uniqueness = KEYS[3]
      local failed = KEYS[4]
      local failedError = KEYS[5]
      local failedTime = KEYS[6]

      local newjob = 0

      if jobHash and jobHash ~= "0" then
         newjob = redis.call('hsetnx', uniqueness, jobHash, 1)
      else
         newjob = 1
      end

      if newjob ~= 0 then
         redis.call('lpush', queue, job)
         redis.call('publish', chann, jobName)
         redis.call('hdel', failed, failureHash) 
         redis.call('hdel', failedError, failureHash) 
         redis.call('zrem', failedTime, failureHash) 
      end 
      ]] 
      return script, 6, QUEUE .. queue, CHANNEL .. queue, UNIQUE .. queue, common.FAILED, common.FAILED_ERROR, common.FAILEDTIME, jobJson, jobName, failureHash, jobHash, cb
   end,


   -- standard dequeue.  move from queue to running list
   dequeue = function(queue, workername, cb)

      local script = [[
      
      local queue =  KEYS[1]
      local runningJobs = KEYS[2]
      local runningSince = KEYS[3]

      local workername = ARGV[1]
      local currenttime = ARGV[2] 

      local job = redis.call('rpop', queue)

      if job then
         redis.call('hset', runningJobs, workername, job)
         redis.call('hset', runningSince, workername, currenttime)
      end
      return job
      ]] 

      return script, 3, QUEUE..queue, common.RUNNING, common.RUNNINGSINCE, workername, os.time(), cb
   end,

   -- if crash, cleanup by moving job from runnning list to failed list
   failure = function(workername, queue, jobHash, errormessage, cb)
      local script = [[
      local workername = ARGV[1]
      local queue = ARGV[2]
      local jobhash = ARGV[3]
      local errormessage = ARGV[4]
      local currenttime = ARGV[5]

      local runningJobs = KEYS[1]
      local failedJobs = KEYS[2]
      local failureReasons = KEYS[3]
      local failureTimes = KEYS[4]


      local job = redis.call('hget', runningJobs, workername)

      local failureHash = queue .. ":" .. jobhash
      redis.call('hset', failedJobs, failureHash, job)
      redis.call('hset', failureReasons, failureHash, errormessage)
      redis.call('zadd', failureTimes, 0 - currenttime, failureHash)

      return redis.call('hdel', runningJobs, workername)
      ]]

      return script, 4, common.RUNNING, common.FAILED, common.FAILED_ERROR, common.FAILEDTIME, workername, QUEUE .. queue, jobHash, errormessage, os.time(), cb
   end,

   -- after successful completion, remove job from running and uniqueness hash (if necessary)
   -- dont remove running time since we want to use that to say when the worker last had a job
   cleanup = function(queue, workername, jobHash, cb)

      -- job's done, take it off the running list, worker is no longer busy

      local script = [[
      local runningJobs = KEYS[1]
      local uniqueness = KEYS[2]
      local workername = ARGV[1]
      local jobHash = ARGV[2]

      redis.call('hdel', runningJobs, workername)
      if jobHash ~= "0" then
         redis.call('hdel', uniqueness, jobHash)
      end
      ]]

      return script, 2, common.RUNNING, UNIQUE .. queue, workername, jobHash, cb
   end,
}

function regularQueue.subscribe(queue, jobs, cb)
   for jobname,job in pairs(jobs) do
      queue.jobs[jobname] = job  
   end

   queue.environment.subscriber.subscribe(CHANNEL .. queue.name, function(message)
      queue.dequeueAndRun()
   end)

   queue.donesubscribing(cb)
end

function regularQueue.enqueue(queue, jobName, argtable, cb)

   local job = {queue = QUEUE .. queue.name, name = jobName, args = argtable.jobArgs}

   local jobHash = argtable.jobHash

   if jobHash and jobHash ~= 0 then
      job.hash = jobName .. jobHash
   else
      job.hash = "0"
   end

   jobHash = job.hash

   local jobJson = json.encode(job)

   cb = cb or function() end

   queue.environment.redis.eval(evals.enqueue(queue.name, jobJson, jobName, jobHash, cb))
end

function regularQueue.reenqueue(queue, argtable, cb)
   queue.environment.redis.eval(evals.reenqueue(argtable.queueName, argtable.jobJson, argtable.jobName, argtable.jobHash, argtable.failureId, cb))
end

function regularQueue.dequeue(queue, cb)

   queue.environment.redis.eval(evals.dequeue(queue.name, queue.environment.workername, function(response) 
      
      local response = response and json.decode(response)
      cb(response)
   end))
end

function regularQueue.failure(queue, argtable, cb)
   queue.environment.redis.eval(evals.failure(queue.environment.workername, queue.name, argtable.jobHash, argtable.err, cb))
end

function regularQueue.cleanup(queue, argtable, cb)
   queue.environment.redis.eval(evals.cleanup(queue.name, queue.environment.workername, argtable.jobHash, cb))
end

return regularQueue
