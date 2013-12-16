local json = require 'cjson'
local async = require 'async'
local redisasync = require 'redis-async'

local qc = require 'redis-queue.config'

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

-- delayed queue (for scheduled execution)
local DELQUEUE = "DELQUEUE:" -- Zset job hash & execution time
local DELCHANNEL = "DELCHANNEL:" --  notify workers of new jobs on channel
local DELJOBS = "DELJOBS:"  -- Hash jobHash => jobJson

-- reserved 
local RUNNING = "RESERVED:RUNNINGJOBS" -- hash
local RUNNINGSINCE = "RESERVED:RUNNINGTIMES" -- hash
local FAILED = "RESERVED:FAILEDJOBS" -- hash 
local FAILED_ERROR = "RESERVED:FAILEDERROR" -- hash 
local FAILEDTIME = "RESERVED:FAILEDTIME" -- hash 

local WAITSTRING = "RESERVED_MESSAGE_WAIT" -- indicates that delayed queue has no jobs ready

-- queue Types

local TYPEQUEUE = "queue"
local TYPELBQUEUE = "lbqueue"
local TYPEDELQUEUE = "delqueue"

-- other constants
local INCREMENT = "INC"

local ILLEGAL_ARGS = {
   "name",
   "instance",
   "queue",
   "args",
   "hash",
}


RedisQueue = {meta = {}}

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

               table.insert(failureTimeList, failureHash)
               table.insert(failureTimeList, currenttime)

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
            redis.call('hmset', failedTime, unpack(failureTimeList))
         end

         jobsCleaned = #deadJobs
      end

      return jobsCleaned
      ]]
      return script, 5, RUNNING, RUNNINGSINCE, FAILED, FAILED_ERROR, FAILEDTIME, os.time(), cb
   end,

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

   reenqueue = function(queue, jobJson, jobName, failureHash, jobHash, cb)
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
         redis.call('hdel', failedTime, failureHash) 
      end 
      ]] 
      return script, 6, QUEUE .. queue, CHANNEL .. queue, UNIQUE .. queue, FAILED, FAILED_ERROR, FAILEDTIME, jobJson, jobName, failureHash, jobHash, cb
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

      return script, 3, QUEUE..queue, RUNNING, RUNNINGSINCE, workername, os.time(), cb
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
      redis.call('hset', failureTimes, failureHash, currenttime)

      return redis.call('hdel', runningJobs, workername)
      ]]

      return script, 4, RUNNING, FAILED, FAILED_ERROR, FAILEDTIME, workername, QUEUE .. queue, jobHash, errormessage, os.time(), cb
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

      return script, 2, RUNNING, UNIQUE .. queue, workername, jobHash, cb
   end,

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
      redis.call('hdel', failedTime, failureHash) 

      ]] 
      return  script, 8, LBQUEUE .. queue, LBCHANNEL .. queue, LBJOBS .. queue, LBBUSY .. queue, LBWAITING .. queue, FAILED, FAILED_ERROR, FAILEDTIME, jobJson, jobName, jobHash, failureHash, priority, cb

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

      return script, 6, LBQUEUE .. queue, LBJOBS .. queue, LBBUSY .. queue, LBWAITING .. queue, RUNNING, RUNNINGSINCE, workername, os.time(), cb
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
      redis.call('hset', failureTimes, failureHash, currenttime)

      return redis.call('hdel', runningJobs, workername)
      ]]

      return script, 4, RUNNING, FAILED, FAILED_ERROR, FAILEDTIME, workername, LBQUEUE .. queue, jobHash, errormessage, os.time(), cb
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

      return script, 4, RUNNING, LBBUSY .. queue, LBWAITING .. queue, LBJOBS .. queue, workername, jobHash, cb
   end,
     
   -- like an lb queue, but without worrying about collisions -- the jobhash is the scheduled time and the job's unique hash
   -- to allow multiple identical jobs to be scheduled
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
      redis.call('hdel', failedTime, failureHash) 

      ]] 
      return  script, 6, DELQUEUE .. queue, DELCHANNEL .. queue, DELJOBS .. queue, FAILED, FAILED_ERROR, FAILEDTIME, jobJson, jobHash, failureHash, os.time(), cb

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

      return script, 4, DELQUEUE .. queue, DELJOBS .. queue, RUNNING, RUNNINGSINCE, workername, WAITSTRING, os.time(), cb
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
      redis.call('hset', failureTimes, failureHash, currenttime)

      return redis.call('hdel', runningJobs, workername)
      ]]

      return script, 4, RUNNING, FAILED, FAILED_ERROR, FAILEDTIME, workername, DELQUEUE .. queue, jobHash, errormessage, os.time(), cb
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

      return script, 2, DELQUEUE .. queue, RUNNING, workername, jobHash, cb
   end,

}


local function checkArgs(args)
   argsjson = json.encode(args)

   for i,arg in ipairs(ILLEGAL_ARGS) do
      local key = argsjson:find('"' .. arg .. '":')
      if key then
         error("Illegal string in arguments table: " .. arg)
      end
   end
end

function RedisQueue:enqueue(queue, jobName, argtable, jobHash)

   checkArgs(argtable)
   local qType = self.config:getqueuetype(queue)

   if qType ~= "QUEUE" then
      error("WRONG TYPE OF QUEUE")
   end

   local job = {queue = QUEUE .. queue, name = jobName, args = argtable}

   if jobHash and jobHash ~= 0 then
      job.hash = jobName .. jobHash
   else
      job.hash = "0"
   end

   jobHash = job.hash

   local jobJson = json.encode(job)

   self.redis.eval(evals.enqueue(queue, jobJson, jobName, jobHash, function(res) return end))
end

-- this enqueues a job on a priority queue.  this way more identical jobs raises the priority of that job
-- if given a priority, sets that priority explicitly
function RedisQueue:lbenqueue(queue, jobName, argtable, jobHash, priority, cb)
   
   checkArgs(argtable)

   local qType = self.config:getqueuetype(queue)

   if qType ~= "LBQUEUE" then
      error("WRONG TYPE OF QUEUE")
   end

   -- instance allows multiple identical jobs to sit on the waiting set
   local job = { queue = LBQUEUE .. queue, name = jobName, args = argtable, instance = async.hrtime()}

   -- job.hash must be a string for dequeue logic
   if jobHash then
      job.hash = jobName .. jobHash
   else
      error("a hash value is require for load balance queue")
   end
      
   jobHash = job.hash

   if type(priority) == "function" then
      cb = priority
      priority = INCREMENT
   else
      priority = priority or INCREMENT
      cb = cb or function(res) return end
   end

   local jobJson = json.encode(job)
   self.redis.eval(evals.lbenqueue(queue, jobJson, jobName, jobHash, priority, cb))
end

function RedisQueue:delenqueue(queue, jobName, argtable, jobHash, timestamp, cb)
   
   checkArgs(argtable)

   local qType = self.config:getqueuetype(queue)

   if qType ~= "DELQUEUE" then
      error("WRONG TYPE OF QUEUE")
   end

   local job = { queue = DELQUEUE .. queue, name = jobName, args = argtable}

   -- job.hash must be a string for dequeue logic
   if jobHash then
      job.hash = jobName .. jobHash
   else
      error("a hash value is require for delayed queue")
   end
      
   jobHash = job.hash

   if type(timestamp) == "function" then
      cb = timestamp
      timestamp = os.time()
   else
      timestamp = timestamp or os.time()
      cb = cb or function(res) return end
   end

   local jobJson = json.encode(job)
   self.redis.eval(evals.delenqueue(queue, jobJson, jobName, jobHash, timestamp, cb))
end

function RedisQueue:reenqueue(failureId, jobJson, cb)
   local _,_,queueArg = jobJson:find('"queue":"(.-)"')
   local _,_,qType,queue = queueArg:find('(.*):(.*)')

   local _,_,jobHash = jobJson:find('"hash":"(.-)"')
   local _,_,jobName = jobJson:find('"name":"(.-)"')

   if qType == "LBQUEUE" then
      -- we use increment as the retry because you want it to run immediately, presumably.
      self.redis.eval(evals.lbreenqueue(queue, jobJson, jobName, jobHash, failureId, INCREMENT, cb))
   elseif qType == "DELQUEUE" then
      self.redis.eval(evals.delreenqueue(queue, jobJson, jobName, jobHash, failureId,cb))
   else
      self.redis.eval(evals.reenqueue(queue, jobJson, jobName, failureId, jobHash, cb))
   end
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
      failureFunct = evals.lbfailure
      cleanupFunct = evals.lbcleanup
   elseif queueType == TYPEDELQUEUE then
      dequeueFunct = evals.deldequeue
      failureFunct = evals.delfailure
      cleanupFunct = evals.delcleanup
      -- delayed queue only sets this true when it's verified that there's another job ready
      self.queuesWaiting[queue] = false
   end

   self.redis.eval(dequeueFunct(queue, self.workername, function(response)
      
      -- in delayed queue the second response value is the timestamp of the next job
      -- setting this outside the fiber since there was a segfault when it was in the fiber
      if queueType == TYPEDELQUEUE then
         -- set the next timeout if necessary
         local nexttimeout = response[2] and tonumber(response[2])
         if nexttimeout and nexttimeout <= os.time() then
            -- no need to wait for a timeout
            self.queuesWaiting[queue] = true
         else
            self:setJobTimeout(queue, nexttimeout)
         end
      end

      async.fiber(function()
         if response then

            local res = response

            --print(pretty.write(res))

            if type(response) == "table" then
               res = response[1]

            end

            if queueType ~= TYPEDELQUEUE or res ~= WAITSTRING then
               res = json.decode(res)


               -- run the function associated with this job
               self.state = "Running:" .. res.name

               -- run it in a pcall

               xpcall(function()
                  self.jobs[res.name](res.args)
               end,
                  function(er)
                     err = debug.traceback(er)
                     print(err) 
                     local failureHash
                     if res.hash == "0" then
                        failureHash = res.name .. ":" .. json.encode(res.args)
                     else
                        failureHash = res.hash
                     end

                     self.redis.eval(failureFunct(self.workername, queue, failureHash, err, function(res)
                        print("ERROR ON JOB " .. err )
                        print("ATTEMPTED CLEANUP: REDIS RESPONSE " .. res)
                     end))      
                  end
               )

               -- call the custom cleanup code for this type of queue
               self.redis.eval(cleanupFunct(queue, self.workername, res.hash, function(response)
                  if queueType == TYPEDELQUEUE and response then
                     local nexttimeout = response
                     self:setJobTimeout(queue, nexttimestamp)
                  end
               end))

            end
            self.state = "Ready"
            self.busy = false
         else
            -- if we take a nil message off the queue, there's nothing left to process on that queue
            self.state = "Ready"
            self.busy = false
            self.queuesWaiting[queue] = false
         end

         -- job's completed, let's check for other jobs we might have missed
         for q,waiting in pairs(self.queuesWaiting) do
            if waiting then
               self:dequeueAndRun(q, queueType)
            end
         end
      end)
   end))
end

function RedisQueue:subscribeJob(queue, jobname, cb)
   if self.jobs[jobname] or self.subscribedQueues[queue] then
      -- don't need to resubscribe, just change the callback
      self.jobs[jobname] = cb 
      self.subscribedQueues[queue] = TYPEQUEUE
   else
      self.jobs[jobname] = cb 
      self.subscriber.subscribe(CHANNEL .. queue, function(message)
         -- new job on the queue
         self:dequeueAndRun(queue)
      end)
      self.subscribedQueues[queue] = TYPEQUEUE
   end
end

function RedisQueue:subscribeLBJob(queue, jobname, cb)

   if self.jobs[jobname] or self.subscribedQueues[queue] then
      -- don't need to resubscribe, just change the callback
      self.jobs[jobname] = cb 
      self.subscribedQueues[queue] = TYPELBQUEUE
   else
      self.jobs[jobname] = cb 
      self.subscriber.subscribe(LBCHANNEL .. queue, function(message)
         -- new job on the queue
         self:dequeueAndRun(queue, TYPELBQUEUE)
      end)
      self.subscribedQueues[queue] = TYPELBQUEUE
   end

end

function RedisQueue:subscribeDELJob(queue, jobname, cb)

   if self.jobs[jobname] or self.subscribedQueues[queue] then
      -- don't need to resubscribe, just change the callback
      self.jobs[jobname] = cb 
      self.subscribedQueues[queue] = TYPEDELQUEUE
   else
      self.jobs[jobname] = cb 
      self.subscriber.subscribe(DELCHANNEL .. queue, function(message)

         local nexttimestamp = tonumber(message[3])
         if nexttimestamp <= os.time() then
            --shortcut to execution
            self.nexttimestamp = nil
            self.nextjobtimeout = nil
            self:dequeueAndRun(queue, TYPEDELQUEUE)
         else
            self:setJobTimeout(queue, nexttimestamp)
         end
      end)
      self.subscribedQueues[queue] = TYPEDELQUEUE
   end

end

function RedisQueue:setJobTimeout(queue, nexttimestamp, cb)
   if nexttimestamp and (self.nexttimestamp == nil or self.nexttimestamp > nexttimestamp) then
      if self.nextjobtimeout then
         self.nextjobtimeout:clear()
      end

      self.nexttimestamp = nexttimestamp

      local now = os.time()

      --want a minimum timeout of 1 second in case of race condition where scheduling machine is 1s faster than worker
      self.nextjobtimeout = async.setTimeout(math.max(nexttimestamp - os.time(), 1) * 1000, function()
         local ts = os.time()
         self.nexttimestamp = nil
         self.nextjobtimeout = nil
         self:dequeueAndRun(queue, TYPEDELQUEUE)
      end)
   end

end

-- please call this at the end of subscribing
function RedisQueue:doneSubscribing()
   for queue,qtype in pairs(self.subscribedQueues) do
      self.queuesWaiting[queue] = true
      self:dequeueAndRun(queue, qtype)
   end
end

-- register a new worker -- see if previous worker on this machine exited uncleanly
-- if so: push last job to failed state.  clean out from queue locks.  
function RedisQueue:registerWorker(redisDetails, cb)
   
   -- set the queuesWaiting table so we don't miss messages
   self.queuesWaiting = {}
   self.subscribedQueues = {}

   -- set worker state so we can tell where it's hung up if it's hanging
   self.workerstate = "idle"

   -- get ip and port for redis client, append hi-res time for unique name

   local name = self.redis.sockname.address .. ":" .. self.redis.sockname.port .. ":" .. async.hrtime()*10000

   -- do cleanup in case dead workers are locking the queues
   self.redis.eval(evals.newworker(function(res) 
      self.redis.client('SETNAME', name, function(res)
         self.workername = name
         -- we need a separate client for handling subscriptions

         redisasync.connect(redisDetails, function(subclient)
            self.subscriber = subclient
            self.subscriber.client('SETNAME', "SUB:" .. name, function(res) end)

            if cb then
               cb()
            end
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

   -- need to fix this so we can wait until the queue is ready
   newqueue.redis = redis
   newqueue.jobs = {}
   newqueue.config = qc(redis)

   setmetatable(newqueue, RedisQueue.meta)
   newqueue.config:fetchConfig(function()
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

