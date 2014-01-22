local async = require 'async'
local redisasync = require 'redis-async'

local qc = require 'redis-queue.config'

local common = require 'redis-queue.common'


RedisQueue = {meta = {}}

RedisQueue.TYPEQUEUE = "queue"
RedisQueue.TYPELBQUEUE = "lbqueue"
RedisQueue.TYPEDELQUEUE = "delqueue"

local queuefactory = require 'redis-queue.queuefactory'

function RedisQueue.meta:__index(key)
   return RedisQueue[key]
end


-- atomic functions

local evals = {

   -- clean up any previous state variables left behind by a crashed worker
   -- all workers that ever did a job will be represented in the runningSince hash 
   -- (it isnt cleared on success or failure, only overwritten on new jobs
   -- so get all workers who did work, see if they left a bad state and clean it up, then clear out their old data
  
   newworker= function(cb)
      local script = [[
      local runningJobs = KEYS[1]
      local runningSince = KEYS[2]
      local failedJobs = KEYS[3]
      local failureErrors = KEYS[4]
      local failedTime = KEYS[5]
   
      local currenttime = ARGV[1]

      local clientList = redis.call('client', 'list')
      local liveWorkers = {}

      local list_index = 1
      while list_index do
         local piss,crap,workerName = clientList:find("name=(.-) ", list_index)
         if workerName then
            liveWorkers[workerName] = true
         end
         list_index = crap
      end

      local deadWorkers = {}
      local allWorkers = redis.call('hkeys', runningSince)
      for i,worker in ipairs(allWorkers) do
         if not liveWorkers[worker] then
            table.insert(deadWorkers, worker)
         end
      end

      local jobsCleaned = 0

      if #deadWorkers > 0 then
           
         local deadJobs = redis.call('hmget', runningJobs, unpack(deadWorkers))
         local newFailedJobs = {}
         local failureReasons = {}
         local failureTimeList = {}

         for i,job in pairs(deadJobs) do
            if job then
               local ass,hole,queue = job:find('"queue":"(.-)"')
               local dung,pee,jobHash =  job:find('"hash":"(.-)"')
               local piss,crap,queuetype,queuename = queue:find("^(.-):(.*)$")

               local failureHash = nil

               if jobHash == "0" then
                  local fgh,hgf,args = job:find('"args":{(.-)}')
                  local xyz,zyx,jobName = job:find('"name":"(.-)"')
                  failureHash = queue .. ":" .. jobName .. ":" .. args
               else
                  failureHash = queue .. ":" .. jobHash
               end

               table.insert(newFailedJobs, failureHash)
               table.insert(newFailedJobs, job)

               table.insert(failureReasons, failureHash)
               table.insert(failureReasons, "Job left behind by dead worker")

               table.insert(failureTimeList, 0 - currenttime)
               table.insert(failureTimeList, failureHash)

               if queuetype == "LBQUEUE" and jobHash then
                  redis.call('hdel', "LBBUSY:"..queuename, jobHash)
               elseif queuetype == "QUEUE" and jobHash then 
                  redis.call('hdel', "UNIQUE:"..queuename, jobHash)
               end
            end
         end

         
         redis.call('hdel', runningJobs, unpack(deadWorkers))
         redis.call('hdel', runningSince, unpack(deadWorkers))
         if #newFailedJobs > 0 then
            redis.call('hmset', failedJobs, unpack(newFailedJobs))
            redis.call('hmset', failureErrors, unpack(failureReasons))
            redis.call('zadd', failedTime, unpack(failureTimeList))
         end

         jobsCleaned = #deadJobs
      end

      return jobsCleaned
      ]]
      return script, 5, common.RUNNING, common.RUNNINGSINCE, common.FAILED, common.FAILED_ERROR, common.FAILEDTIME, os.time(), cb
   end,

}


function RedisQueue:enqueueJob(queueName, jobName, jobArgs, moreArgs, cb)
   local queue = self.queues[queueName]
   moreArgs.jobArgs = jobArgs
   queue.enqueue(jobName, moreArgs, cb)
end

-- these enqueues are here for backwards compatibility
function RedisQueue:enqueue(queueName, jobName, argtable, jobHash)
   local queue = self.queues[queueName]
   queue.enqueue(jobName, {jobArgs = argtable, jobHash = jobHash}, cb)
end

function RedisQueue:lbenqueue(queueName, jobName, argtable, jobHash, priority, cb)
   local queue = self.queues[queueName]
   queue.enqueue(jobName, {jobArgs = argtable, jobHash = jobHash, priority = priority}, cb)
end

function RedisQueue:delenqueue(queueName, jobName, argtable, jobHash, timestamp, cb)
   local queue = self.queues[queueName]
   queue.enqueue(jobName, {jobArgs = argtable, jobHash = jobHash, timestamp = timestamp}, cb)
end



function RedisQueue:reenqueue(failureId, jobJson, cb)
   local _,_,queueArg = jobJson:find('"queue":"(.-)"')
   local _,_,qType,queue = queueArg:find('(.*):(.*)')

   local queueObj = self.queues[queue]

   if queue and queueObj then
      queueObj.reenqueue(failureId, jobJson, cb)
   else
      log.print("Error trying to requeue " .. jobJson)
   end
end

-- please call this at the end of subscribing
function RedisQueue:doneSubscribing()
end

function RedisQueue:registerWorker(redisDetails, jobs, cb)

   local name = self.redis.sockname.address .. ":" .. self.redis.sockname.port .. ":" .. async.hrtime()*10000

   self.redis.eval(evals.newworker(function(res) 
      self.redis.client('SETNAME', name, function(res)
         self.workername = name
         redisasync.connect(redisDetails, function(subclient)
            self.subscriber = subclient
            for queuename, jobList in pairs(jobs) do
               local queue = self.queues[queuename]
               queue.subscribe(jobList)
            end
            if cb then cb() end
         end)
      end)
   end))
end

function RedisQueue:close()
   if self.subscriber then
      self.subscriber.close()
   end
end

function RedisQueue:new(redis, cb)
   local newqueue = {}

   newqueue.redis = redis
   newqueue.jobs = {}
   newqueue.config = qc(redis)
   newqueue.queues = {}

   setmetatable(newqueue, RedisQueue.meta)

   newqueue.config:fetchConfig(function()
      queuefactory:init(newqueue)

      local queuemodels = newqueue.config:getqueuemodels()

      for queuename,queuetype in pairs(queuemodels) do
         newqueue.queues[queuename] = queuefactory:newqueue(queuename, queuetype)
      end

      if cb then
         cb(newqueue)
      end
   end)

   return newqueue
end

local rqueue = {
}

setmetatable(rqueue, {
   __call = RedisQueue.new,
})

return rqueue

