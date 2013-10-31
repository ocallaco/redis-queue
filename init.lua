local json = require 'cjson'
local async = require 'async'
local redisasync = require 'redis-async'

-- important keys

local QUEUE = "QUEUE:"
local CHANNEL = "CHANNEL:"
local UNIQUE = "UNIQUE:"
local RUNNING = "RUNNINGJOBS"

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

   local jobJson = json.encode(job)

   -- see if the job is already running.  if not, enqueue it and publish to the channel

   self.redis.eval([[
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
   ]], 3, QUEUE .. queue, CHANNEL .. queue, UNIQUE .. queue, jobJson, jobName, job.hash, function(res) end)
end

function RedisQueue:dequeueAndRun(queue)
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
   self.redis.eval([[
      local job = redis.call('rpop', KEYS[1])
      if job then
         redis.call('hset', KEYS[2], ARGV[1], job)
      end
      return job
   ]], 2, QUEUE..queue, RUNNING, self.workername, function(res)
     
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
            if not ok then print(err) end

            -- job's done, take it off the running list, worker is no longer busy
            self.redis.hdel(RUNNING, self.workername)
            if res.hash then
               self.redis.hdel(UNIQUE .. queue, res.hash)
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
   end)
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

local queue = {}

setmetatable(queue, {
   __call = RedisQueue.new,
})

return queue

