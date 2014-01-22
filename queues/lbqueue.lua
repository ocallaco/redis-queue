local common = require 'redis-queue.common'
local json = require 'cjson'
local async = require 'async'

local LBQUEUE = "LBQUEUE:" -- ZSet job hash & priority
local LBCHANNEL = "LBCHANNEL:" -- notify workers of new jobs on channel
local LBJOBS = "LBJOBS:" -- Hash jobHash => jobJson
local LBBUSY = "LBBUSY:" -- Hash jobHash => workername
local LBWAITING = "LBWAITING:" -- Set

   -- other constants
local INCREMENT = "INC"


local evals = {
   
   -- check if job exists.  if so, see if it's running.  if so, put on waiting list, otherwise, increment it
   -- note: hsetnx() ALWAYS returns integer 1 or 0
   lbenqueue = function(queue, jobJson, jobName, jobHash, priority, cb)
      local script = [[
      local jobJson = ARGV[1]
      local jobName = ARGV[2]
      local jobHash = ARGV[3]
      local priority = ARGV[4]

      local queue = KEYS[1]
      local chann = KEYS[2]
      local jobmatch = KEYS[3]
      local busy = KEYS[4]
      local waiting = KEYS[5]

      local jobExists = redis.call('hsetnx', jobmatch, jobHash, jobJson)

      if jobExists == 0 then
         local isbusy = redis.call('hget', busy, jobHash) 
         if isbusy then
            redis.call('sadd', waiting, jobJson)
            redis.call('publish', chann, jobName)
            return
         end
      end

      if priority == "INC" then 
         redis.call('zincrby', queue, -1, jobHash)
      else
         redis.call('zadd', queue, tonumber(priority), jobHash)
      end

      redis.call('publish', chann, jobName)
      ]] 
      return  script, 5, LBQUEUE .. queue, LBCHANNEL .. queue, LBJOBS .. queue, LBBUSY .. queue, LBWAITING .. queue, jobJson, jobName, jobHash, priority, cb

   end,

     
   lbreenqueue = function(queue, jobJson, jobName, jobHash, failureHash, priority, cb)
      local script = [[
      local jobJson = ARGV[1]
      local jobName = ARGV[2]
      local jobHash = ARGV[3]
      local failureHash = ARGV[4]
      local priority = ARGV[5]

      local queue = KEYS[1]
      local chann = KEYS[2]
      local jobmatch = KEYS[3]
      local busy = KEYS[4]
      local waiting = KEYS[5]
      local failed = KEYS[6]
      local failedError = KEYS[7]
      local failedTime = KEYS[8]

      local jobExists = redis.call('hsetnx', jobmatch, jobHash, jobJson)

      if jobExists == 0 then
         local isbusy = redis.call('hget', busy, jobHash) 
         if isbusy then
            redis.call('sadd', waiting, jobJson)
            redis.call('publish', chann, jobName)
            redis.call('hdel', failed, failureHash) 
            redis.call('hdel', failedError, failureHash) 
            return
         end
      end

      if priority == "INC" then 
         redis.call('zincrby', queue, -1, jobHash)
      else
         redis.call('zadd', queue, tonumber(priority), jobHash)
      end

      redis.call('publish', chann, jobName)
      redis.call('hdel', failed, failureHash) 
      redis.call('hdel', failedError, failureHash) 
      redis.call('zrem', failedTime, failureHash) 

      ]] 
      return  script, 8, LBQUEUE .. queue, LBCHANNEL .. queue, LBJOBS .. queue, LBBUSY .. queue, LBWAITING .. queue, common.FAILED, common.FAILED_ERROR, common.FAILEDTIME, jobJson, jobName, jobHash, failureHash, priority, cb

   end,
      
   -- check for waiting jobs.  see if they're on the busy list.  if not, increment them on the queue
   -- note, could be more efficient by tallying up the times i see a hash and zincrby only once per hash
   -- but this is already pretty elaborate.  don't want more moving parts to get me confused right now
   -- take top job off queue and return it
   lbdequeue = function(queue, workername, cb)
      script = [[
         local queue = KEYS[1]
         local jobs = KEYS[2]
         local busy = KEYS[3]
         local waiting = KEYS[4]
         local running = KEYS[5]
         local runningSince = KEYS[6]

         local workername = ARGV[1]
         local currenttime = ARGV[2] 

         local waitingJobs = redis.call('smembers', waiting)

         if #waitingJobs > 0 then
            local waitingHashes = {}
            for i,job in ipairs(waitingJobs) do
               local shit, crap, jobHash = job:find('"hash":"(.-)"')
               table.insert(waitingHashes, jobHash)
            end

            local busyList = {}

            if #waitingHashes > 0 then
               busyList = redis.call('hmget', busy, unpack(waitingHashes))
            end

            local readyJobHashes = {}
            local readyJobs = {}

            for i,jobHash in ipairs(waitingHashes) do
               if not busyList[i] then
                  table.insert(readyJobHashes, jobHash)
                  table.insert(readyJobs, waitingJobs[i])
               end
            end

            if #readyJobs > 0 then 
               redis.call('srem', waiting, unpack(readyJobs))
            end

            for i,jobHash in ipairs(readyJobHashes) do
               redis.call('zincrby', queue, -1, jobHash)
            end
         end
  

         local topJobHash = redis.call('zrange', queue, 0, 0)[1]
         redis.call('zremrangebyrank', queue, 0, 0)

         local topJob = nil

         if topJobHash then
            topJob = redis.call('hget', jobs, topJobHash)
            redis.call('hset', running, workername, topJob)
            redis.call('hset', runningSince, workername, currenttime)
            local xfd = redis.call('hset', busy, topJobHash, 1)
         end

         return topJob

      ]]

      return script, 6, LBQUEUE .. queue, LBJOBS .. queue, LBBUSY .. queue, LBWAITING .. queue, common.RUNNING, common.RUNNINGSINCE, workername, os.time(), cb
   end,
    -- this needs to be built out better 
   lbfailure = function(workername, queue, jobHash, errormessage, cb)
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

      return script, 4, common.RUNNING, common.FAILED, common.FAILED_ERROR, common.FAILEDTIME, workername, LBQUEUE .. queue, jobHash, errormessage, os.time(), cb
   end,
            
   lbcleanup = function(queue, workername, jobHash, cb)

      -- job's done, take it off the running list, worker is no longer busy
      -- dont remove running time since we want to use that to say when the worker last had a job

      local script = [[
      local runningJobs = KEYS[1]
      local busy = KEYS[2]
      local waiting = KEYS[3]
      local jobs = KEYS[4]

      local workername = ARGV[1]
      local jobHash = ARGV[2]

      redis.call('hdel', runningJobs, workername)
      
      local xfd = redis.call('hdel', busy, jobHash)

      local waitingJobs = redis.call('smembers', waiting)
      
      for i,job in ipairs(waitingJobs) do
         local shit, crap, waitingJobHash = job:find('"hash":"(.-)"')
         if waitingJobHash == jobHash then
            return 1
         end
      end

      redis.call('hdel', jobs, jobHash)
      return 2

      ]]

      return script, 4, common.RUNNING, LBBUSY .. queue, LBWAITING .. queue, LBJOBS .. queue, workername, jobHash, cb
   end,
     
}

local lbqueue = {}

function lbqueue.subscribe(queue, jobs, cb)

   async.fiber(function()
      for jobname, job in pairs(jobs) do
        
         if type(job) == 'table' and job.prepare then
            if job.prepare then
               wait(job.prepare, {})
            end
            queue.jobs[jobname] = job.run
            print(queue.jobs)
         else
            queue.jobs[jobname] = job
         end
      end

      queue.environment.subscriber.subscribe(LBCHANNEL .. queue.name, function(message)
         queue.dequeueAndRun()
      end)

      queue.donesubscribing(cb)
   end)
end

function lbqueue.enqueue(queue, jobName, argtable, cb)

   -- instance allows multiple identical jobs to sit on the waiting set
   local job = { queue = LBQUEUE .. queue.name, name = jobName, args = argtable.jobArgs, instance = async.hrtime()}
   local jobHash = argtable.jobHash

   -- job.hash must be a string for dequeue logic
   if jobHash then
      job.hash = jobName .. jobHash
   else
      error("a hash value is require for load balance queue")
   end
      
   jobHash = job.hash

   local priority = argtable.priority

   priority = priority or INCREMENT
   cb = cb or function(res) return end

   local jobJson = json.encode(job)
   queue.environment.redis.eval(evals.lbenqueue(queue.name, jobJson, jobName, jobHash, priority, cb))
end

function lbqueue.reenqueue(queue, argtable, cb)
   queue.environment.redis.eval(evals.lbreenqueue(argtable.queueName, argtable.jobJson, argtable.jobName, argtable.jobHash, argtable.failureId, INCREMENT, cb))
end

function lbqueue.dequeue(queue, cb)

   queue.environment.redis.eval(evals.lbdequeue(queue.name, queue.environment.workername, function(response) 
      local response = response and json.decode(response)
      cb(response)
   end))
end

function lbqueue.failure(queue, argtable,cb)
   queue.environment.redis.eval(evals.lbfailure(queue.environment.workername, queue.name, argtable.jobHash, argtable.err, cb))
end

function lbqueue.cleanup(queue, argtable, cb)
   queue.environment.redis.eval(evals.lbcleanup(queue.name, queue.environment.workername, argtable.jobHash, cb))
end

return lbqueue
