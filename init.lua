local json = require 'cjson'
local async = require 'async'
local redisasync = require 'redis-async'

-- standard queues
local QUEUE = "QUEUE:"
local CHANNEL = "CHANNEL:"
local UNIQUE = "UNIQUE:"

-- load balance queues
local LBQUEUE = "LBQUEUE:" -- ZSet job hash & priority
local LBCHANNEL = "LBCHANNEL:" -- notify workers of new jobs on channel
local LBJOBS = "LBJOBS:" -- Hash jobHash => jobJson
local LBBUSY = "LBBUSY:" -- Hash jobHash => workername
local LBWAITING = "LBWAITING:" -- Set

-- reserved 
local RUNNING = "RESERVED:RUNNINGJOBS"
local FAILED = "RESERVED:FAILED"

local WORKERCHANNEL = "RESERVEDCHANNEL:WORKER"


-- queue Types

local TYPEQUEUE = "queue"
local TYPELBQUEUE = "lbqueue"

-- atomic functions

local evals = {

   enqueue = function(queue, jobJson, jobName, jobHash, cb)

      local script = [[
      local job = ARGV[1]
      local jobName = ARGV[2]
      local jobHash = ARGV[3]

      local newjob = 0

      if jobHash and jobHash ~= 0 then
         newjob = redis.call('hsetnx', KEYS[3], jobHash, 1)
      else
         newjob = 1
      end

      if newjob ~= 0 then
         redis.call('lpush', KEYS[1], job)
         redis.call('publish', KEYS[2], jobName)
      end
      ]] 
      return script, 3, QUEUE .. queue, CHANNEL .. queue, UNIQUE .. queue, jobJson, jobName, jobHash, cb
   end,


   dequeue = function(queue, workername, cb)

      local script = [[
      local job = redis.call('rpop', KEYS[1])
      if job then
         redis.call('hset', KEYS[2], ARGV[1], job)
      end
      return job
      ]] 

      return script, 2, QUEUE..queue, RUNNING, workername, cb
   end,


   failure = function(workername, cb)
      local script = [[
      local workername = ARGV[1]
      local runningJobs = KEYS[1]
      local failedJobs = KEYS[2]

      local job = redis.call('hget', runningJobs, workername)
      redis.call('lpush', failedJobs, job)
      return redis.call('hdel', runningJobs, workername)
      ]]

      return script, 2, RUNNING, FAILED, workername, cb
   end,

            
   cleanup = function(queue, workername, jobHash, cb)

      -- job's done, take it off the running list, worker is no longer busy

      local script = [[
      local runningJobs = KEYS[1]
      local uniqueness = KEYS[2]
      local workername = ARGV[1]
      local jobHash = ARGV[2]

      redis.call('hdel', runningJobs, workername)
      if jobHash ~= 0 then
         redis.call('hdel', uniqueness, jobHash)
      end
      ]]

      return script, 2, RUNNING, UNIQUE .. queue, workername, jobHash, cb
   end,

   
   lbenqueue = function(queue, jobJson, jobName, jobHash, cb)
      local script = [[
      local jobJson = ARGV[1]
      local jobName = ARGV[2]
      local jobHash = ARGV[3]

      local queue = KEYS[1]
      local chann = KEYS[2]
      local jobmatch = KEYS[3]
      local busy = KEYS[4]
      local waiting = KEYS[5]

      local jobExists = redis.call('hsetnx', jobmatch, jobHash, jobJson)

      if jobExists =~ 0 then
         local isbusy = redis.call('hget', busy, jobHash) 
         if isbusy == 1 then
            redis.call('sadd', waiting, jobJson)
            redis.call('publish', chann, jobName)
            return
         end
      end

      redis.call('zincrby', queue, -1, jobHash)
      redis.call('publish', chann, jobName)
      ]] 
      return  script, 5, LBQUEUE .. queue, LBCHANNEL .. queue, LBJOBS .. queue, LBBUSY .. queue, LBWAITING .. queue, jobJson, jobName, jobHash, cb

   end,

   -- check for waiting jobs.  add any to the queue if possible
   -- take top job off queue and return it
   lbdequeue = function(queue, workername, cb)
      script = [[
      ]]

      return script, LBQUEUE .. queue,  cb

   end,
         
   lbcleanup = function(queue, workername, jobHash, cb)

      -- job's done, take it off the running list, worker is no longer busy

      local script = [[
      local runningJobs = KEYS[1]
      local uniqueness = KEYS[2]
      local workername = ARGV[1]
      local jobHash = ARGV[2]

      redis.call('hdel', runningJobs, workername)
      if jobHash ~= 0 then
         redis.call('hdel', uniqueness, jobHash)
      end
      ]]

      return script, 2, RUNNING, UNIQUE .. queue, workername, jobHash, cb
   end,
}

RedisQueue = {meta = {}}

function RedisQueue.meta:__index(key)
   return RedisQueue[key]
end

function RedisQueue:enqueue(queue, jobName, argtable, jobHash)
   local job = { name = jobName, args = argtable}

   if jobHash then
      job.hash = jobName .. jobHash
   else
      job.hash = 0
   end

   jobHash = job.hash

   local jobJson = json.encode(job)

   self.redis.eval(evals.enqueue(queue, jobJson, jobName, jobHash, function(res) return end))
end

-- this enqueues a job on a priority queue.  this way more identical jobs raises the priority of that job
function RedisQueue:lbenqueue(queue, jobName, argtable, jobHash)
   local job = { name = jobName, args = argtable}

   local jobJson = json.encode(job)

   if jobHash then
      job.hash = jobName .. jobHash
   else
      error("a hash value is require for load balance queue")
   end
      
   jobHash = job.hash

   self.redis.eval(evals.lbenqueue(queue, jobJson, jobName, jobHash, function(res) return end))
end

function RedisQueue:dequeueAndRun(queue, queueType)

   if not queueType then
      queueType = "queue"
   end

   -- atomically pop the job and push it onto an hset

   -- if the worker is busy, set a reminder to check that queue when done processing, otherwise, process it
   if self.busy or self.workername == nil then
      self.queuesWaiting[queue] = true
      return
   end
   
   -- need to set this before pulling a job off the queue to ensure one job at a time
   self.busy = true

   -- not busy, so atomically take job off the queue, 
   -- put it in the RUNNINGJOBS hash under the current worker's name

   self.state = "Dequeuing job"


   local dequeueFunct, failureFunct, cleanupFunct


   if queueType == TYPEQUEUE then
      dequeueFunct = evals.dequeue
      failureFunct = evals.failure
      cleanupFunct = evals.cleanup
   elseif queueType == TYPELBQUEUE then
      dequeueFunct = evals.lbdequeue
      failureFunct = evals.failure
      cleanupFunct = evals.lbcleanup
   end

   self.redis.eval(evals.dequeue(queue, self.workername, function(res)
      async.fiber(function()
         if res then

            res = json.decode(res)

            -- run the function associated with this job
            self.state = "Running:" .. res.name

            -- run it in a pcall
            local ok, err = pcall(function()
               self.jobs[res.name](res.args)
            end)

            -- if not ok, the pcall crashed -- report the error...
            if not ok then 
               print(err) 
               self.redis.eval(failureFunct(self.workername, function(res)
                  print("ERROR ON JOB " .. err )
                  print("ATTEMPTED CLEANUP: REDIS RESPONSE " .. res)
               end))
            else
               -- call the custom cleanup code for this type of queue
               self.redis.eval(cleanupFunct(queue, self.workername, res.hash, function() end))
            end

            self.busy = false
            self.state = "Ready"

         else
            -- if we take a nil message off the queue, there's nothing left to process on that queue
            self.state = "Ready"
            self.busy = false
            self.queuesWaiting[queue] = false
         end

         -- job's completed, let's check for other jobs we might have missed
         for q,waiting in pairs(self.queuesWaiting) do
            if waiting then
               self:dequeueAndRun(q)
            end
         end
      end)
   end))
end

function RedisQueue:lbdequeueAndRun(queue)

end

function RedisQueue:subcribeJob(queue, jobname, cb)
   if self.jobs[jobname] or self.subscribedQueues[queue] then
      -- don't need to resubscribe, just change the callback
      self.jobs[jobname] = cb 
      self.subscribedQueues[queue] = true
   else
      self.jobs[jobname] = cb 
      self.subscriber.subscribe(CHANNEL .. queue, function(message)
         -- new job on the queue
         self:dequeueAndRun(queue)
      end)
      
      -- check the queue immediately on subscription
      self:dequeueAndRun(queue)
   end
end

function RedisQueue:registerWorker(redisDetails, cb)
   
   -- set the queuesWaiting table so we don't miss messages
   self.queuesWaiting = {}
   self.subscribedQueues = {}

   -- set worker state so we can tell where it's hung up if it's hanging
   self.workerstate = "idle"

   -- get ip and port for redis client, append hi-res time for unique name

   local name = self.redis.sockname.address .. ":" .. self.redis.sockname.port .. ":" .. async.hrtime()*10000
   self.redis.client('SETNAME', name, function(res)
      self.workername = name
   end)
   
   -- we need a separate client for handling subscriptions

   redisasync.connect(redisDetails, function(subclient)
      self.subscriber = subclient
      self.subscriber.client('SETNAME', "SUB:" .. name, function(res) end)

      if cb then
         cb()
      end
   end)
end

function RedisQueue:close()
   if self.subscriber then
      self.subscriber.close()
   end
end

function RedisQueue:new(redis, ...)
   local newqueue = {}
   newqueue.redis = redis
   newqueue.jobs = {}
   setmetatable(newqueue, RedisQueue.meta)
  
--   newqueue:registerWorker()

   return newqueue
end

local rqueue = {
}

setmetatable(rqueue, {
   __call = RedisQueue.new,
})

return rqueue

