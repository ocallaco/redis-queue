local async = require 'async'
local json = require 'cjson'
local common = require 'redis-queue.common'

-- delayed queue (for scheduled execution)
local DELQUEUE = "DELQUEUE:" -- Zset job hash & execution time
local DELCHANNEL = "DELCHANNEL:" --  notify workers of new jobs on channel
local DELJOBS = "DELJOBS:"  -- Hash jobHash => jobJson
local WAITSTRING = "RESERVED_MESSAGE_WAIT" -- indicates that delayed queue has no jobs ready

local evals = {
   
   -- like an lb queue, but without worrying about collisions -- the jobhash is the scheduled time and the job's unique hash
   -- to allow multiple identical jobs to be scheduled
   -- TODO: add cleanup work
   delenqueue = function(queue, jobJson, jobName, jobHash, scheduletime, cb)
      local script = [[
      local jobJson = ARGV[1]
      local jobName = ARGV[2]
      local jobHash = ARGV[3]
      local scheduletime = ARGV[4]

      jobHash = scheduletime .. "|" .. jobHash

      local queue = KEYS[1]
      local chann = KEYS[2]
      local jobmatch = KEYS[3]

      local jobExists = redis.call('hsetnx', jobmatch, jobHash, jobJson)

      if jobExists == 1 then
         redis.call('zadd', queue, tonumber(scheduletime), jobHash)
         redis.call('publish', chann, scheduletime)
      end
      ]] 
      return  script, 3, DELQUEUE .. queue, DELCHANNEL .. queue, DELJOBS .. queue, jobJson, jobName, jobHash, scheduletime, cb

   end,

   delreenqueue = function(queue, jobJson, jobName, jobHash, failureHash, cb)
      local script = [[
      local jobJson = ARGV[1] 
      local jobHash = ARGV[2] 
      local failureHash = ARGV[3] 
      local currenttime = ARGV[4]

      local queue = KEYS[1] 
      local chann = KEYS[2] 
      local jobmatch = KEYS[3] 
      local failed = KEYS[4] 
      local failedError = KEYS[5] 
      local failedTime = KEYS[6] 

      jobHash = currenttime .. "|" .. jobHash 

      redis.call('zadd', queue, currenttime, jobHash)
      redis.call('hset', jobmatch, jobHash, jobJson)

      redis.call('publish', chann, currenttime)
      redis.call('hdel', failed, failureHash) 
      redis.call('hdel', failedError, failureHash) 
      redis.call('zrem', failedTime, failureHash) 

      ]] 
      return  script, 6, DELQUEUE .. queue, DELCHANNEL .. queue, DELJOBS .. queue, common.FAILED, common.FAILED_ERROR, common.FAILEDTIME, jobJson, jobHash, failureHash, os.time(), cb

   end,

   -- dequeue any job that's ready to run, otherwise, return the time of the next job to run (or nil if there are none scheduled)
   deldequeue = function(queue, workername, cb)
      script = [[
      local queue = KEYS[1]
      local jobs = KEYS[2]
      local running = KEYS[3]
      local runningSince = KEYS[4]

      local workername = ARGV[1]
      local waitstring = ARGV[2] 
      local currenttime = ARGV[3] 

      local jobswaiting = redis.call('ZRANGE', queue, 0, 1, "WITHSCORES")
      if #jobswaiting > 0 and jobswaiting[2] <= currenttime then
         
         local topJobHash = jobswaiting[1]
         redis.call('zremrangebyrank', queue, 0, 0)

         local topJob = redis.call('hget', jobs, topJobHash)
         redis.call('hdel', jobs, topJobHash)
         redis.call('hset', running, workername, topJob)
         redis.call('hset', runningSince, workername, currenttime)

         return {topJob, jobswaiting[4]}
      else
         return {waitstring, jobswaiting[2]}
      end

      ]]

      return script, 4, DELQUEUE .. queue, DELJOBS .. queue, common.RUNNING, common.RUNNINGSINCE, workername, WAITSTRING, os.time(), cb
   end,

       -- this needs to be built out better 
   delfailure = function(workername, queue, jobHash, errormessage, cb)
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

      return script, 4, common.RUNNING, common.FAILED, common.FAILED_ERROR, common.FAILEDTIME, workername, DELQUEUE .. queue, jobHash, errormessage, os.time(), cb
   end,

   delcleanup = function(queue, workername, jobHash, cb)

      -- job's done, take it off the running list, worker is no longer busy
      -- peek at head of the queue to tell worker when to schedule next timeout for

      local script = [[
      local queue = KEYS[1]
      local runningJobs = KEYS[2]

      local workername = ARGV[1]
      local jobHash = ARGV[2]

      redis.call('hdel', runningJobs, workername)

      local nextjob = redis.call('zrange', queue, 0,0, 'WITHSCORES')
      return nextjob[2]

      ]]

      return script, 2, DELQUEUE .. queue, common.RUNNING, workername, jobHash, cb
   end,
}

local delqueue = {}

local setJobTimeout = function(queue, nexttimestamp)
   if nexttimestamp and (queue.nexttimestamp == nil or queue.nexttimestamp > nexttimestamp) then
      if queue.nextjobtimeout then
         queue.nextjobtimeout:clear()
      end

      queue.nexttimestamp = nexttimestamp

      local now = os.time()

      --want a minimum timeout of 1 second in case of race condition where scheduling machine is 1s faster than worker
      queue.nextjobtimeout = async.setTimeout(math.max(nexttimestamp - os.time(), 1) * 1000, function()
         local ts = os.time()
         queue.nexttimestamp = nil
         queue.nextjobtimeout = nil
         queue:dequeueAndRun()
      end)
   end
end

function delqueue.subscribe(queue, jobs, cb)

   for jobname, job in pairs(jobs) do
      queue.jobs[jobname] = job  
   end

   queue.environment.subscriber.subscribe(DELCHANNEL .. queue.name, function(message)

      local nexttimestamp = tonumber(message[3])
      if nexttimestamp <= os.time() then
         --shortcut to execution
         queue.nexttimestamp = nil
         queue.nextjobtimeout = nil
         queue.dequeueAndRun()
      else
         setJobTimeout(queue, nexttimestamp)
      end
   end)

   if queue.intervalSet ~= true then
      async.setInterval(60 * 1000, function() 
         if queue.nexttimestamp == nil or (queue.nexttimestamp and queue.nexttimestamp < os.time()) then
            queue.dequeueAndRun() 
         end

         queue.intervalSet = true
      end)
   end

   queue.donesubscribing(cb)
end

function delqueue.enqueue(queue, jobName, argtable, cb)

   local job = { queue = DELQUEUE .. queue.name, name = jobName, args = argtable.jobArgs}

   local jobHash = argtable.jobHash
   -- job.hash must be a string for dequeue logic
   if jobHash then
      job.hash = jobName .. jobHash
   else
      error("a hash value is require for delayed queue")
   end
      
   jobHash = job.hash

   local timestamp = argtable.timestamp


   timestamp = timestamp or os.time()
   cb = cb or function(res) return end

   local jobJson = json.encode(job)
   queue.environment.redis.eval(evals.delenqueue(queue.name, jobJson, jobName, jobHash, timestamp, cb))
end

function delqueue.reenqueue(queue, argtable, cb)
   queue.environment.redis.eval(evals.delreenqueue(argtable.queueName, argtable.jobJson, argtable.jobName, argtable.jobHash, argtable.failureId, cb))
end

function delqueue.dequeue(queue, cb)

   queue.environment.redis.eval(evals.deldequeue(queue.name, queue.environment.workername, function(response) 

      local nexttimeout = response[2] and tonumber(response[2])
      if nexttimeout and nexttimeout <= os.time() then
         -- no need to wait for a timeout
         queue.waiting = true
      else
         setJobTimeout(queue, nexttimeout)
      end

      if response[1] == nil or response[1] == WAITSTRING then
         cb(nil)
      else
         cb(json.decode(response[1]))
      end
   end))
end


function delqueue.failure(queue, argtable)
   --print("FAILURE", queue.environment.workername, queue.name, argtable.jobHash, argtable.err)
   queue.environment.redis.eval(evals.delfailure(queue.environment.workername, queue.name, argtable.jobHash, argtable.err, cb))
end

function delqueue.cleanup(queue, argtable)
   -- not sure if we should set the next timeout again with cleanup, but it only allows one timeout at a time anyway
   -- might save us some unneeded timeouts.  also, could make this cancel any waiting timeouts if it comes back nil
   queue.environment.redis.eval(evals.delcleanup(queue.name, queue.environment.workername, argtable.jobHash, function(response)
      local nexttimestamp = response and tonumber(response)
      if nexttimestamp then
         setJobTimeout(queue, nexttimestamp)
      end
   end))
end

return delqueue
