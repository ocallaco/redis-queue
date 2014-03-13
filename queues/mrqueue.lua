local common = require 'redis-queue.common'
local json = require 'cjson'
local async = require 'async'

local MRQUEUE = "MRQUEUE:" -- ZSet job hash & priority -- will have form MRQUEUE:<SUB_QUEUE_NUMBER>:<NAME> 
local MRCHANNEL = "MRCHANNEL:" -- notify workers of new jobs on channel
local MRCONFIG = "MRCONFIG:" -- hash -- for now just sets # of subqueues, other things may be customizeable
local MRJOBS = "MRJOBS:" -- Hash jobHash => jobJson
local MRPROGRESS = "MRPROGRESS:" -- Hash jobHash => # of workers left to report (0 means reduce step remains) -- also used like LBBUSY
local MRRESULTS = "MRRESULTS:" -- Hash queue name = subqueue, entry = results hash for reduce step
local MRASSIGNMENT = "MRASSIGNMENT:" -- Hash workername => queuenumber
local MRWAITING = "MRWAITING:" -- Set

   -- other constants
local INCREMENT = "INC"


local evals = {

   --TODO: make it enqueue reduce step if progress comes down to 0
   --TODO: remove dead workers names from assignment
   -- DONE
   startup = function(queue, workername, nodenum, cb)
      local script = [[
         local waiting = KEYS[1]
         local jobmatch = KEYS[2]
         local assignment = KEYS[3]
         local mrconfig = KEYS [4]

         local cleanupPrefix= ARGV[1]
         local queueprefix = ARGV[2]
         local queuename = ARGV[3]
         local workername = ARGV[4]
         local nodenum = tonumber(ARGV[5])

         local queuecount = redis.call('hget', mrconfig, "nqueues")

         for j=0,queuecount do 
            local subqueuename = queueprefix .. j .. ":" .. queuename
            local cleanupName = cleanupPrefix .. subqueuename

            local cleanupJobs = redis.call('lrange', cleanupName, 0, -1)

            for i,cleanupJob in ipairs(cleanupJobs)do
               local x,y,firstpart,jobHash,lastpart = cleanupJob:find('(.*"hash":")(.-)(".*)')
               local rehashedJob = firstpart .. "FAILURE:" .. jobHash .. lastpart

               local x,y,firstpart,jobName,lastpart = rehashedJob:find('(.*"name":")(.-)(".*)')
               local renamedJob = firstpart .. "FAILURE:" .. jobName .. lastpart

               redis.call('hincryby', progress, jobHash, -1)
               redis.call('hset', results, subqueuename, '{"result":"FAILURE","info":{"reason":"DEADWORKER"}}')
               redis.call('hset', jobmatch, "FAILURE:" .. jobHash, renamedJob)
               redis.call('sadd', waiting, renamedJob)
            end

            redis.call('del', cleanupName)
         end

         redis.call('hset', assignment, workername, queueprefix .. nodenum .. ":" .. queuename)
      ]]
      return script, 4, MRWAITING .. queue, MRJOBS .. queue, MRASSIGNMENT .. queue, MRCONFIG .. queue, common.CLEANUP, MRQUEUE, queue, workername, nodenum, cb
   end,
  
   --DONE
   -- check if job exists.  if so, see if it's running.  if so, put on waiting list, otherwise, add to subqueues, clear MRRESULTS, set MRPROGRESS
   -- note: hsetnx() ALWAYS returns integer 1 or 0
   mrenqueue = function(queue, jobJson, jobName, jobHash, priority, cb)
      local script = [[
      local jobJson = ARGV[1]
      local jobName = ARGV[2]
      local jobHash = ARGV[3]
      local priority = ARGV[4]
      local queueprefix = ARGV[5]
      local queuename = ARGV[6]

      local chann = KEYS[1]
      local jobmatch = KEYS[2]
      local progress = KEYS[3]
      local waiting = KEYS[4]
      local results = KEYS[5]
      local mrconfig = KEYS[6]

      local jobExists = redis.call('hsetnx', jobmatch, jobHash, jobJson)

      if jobExists == 0 then
         local progcount = redis.call('hget', progress, jobHash) 
         if progcount and tonumber(progcount) >= 1 then
            redis.call('sadd', waiting, jobJson)
            redis.call('publish', chann, jobName)
            return
         end
      end

      local queuecount = redis.call('hget', mrconfig, "nqueues")

      if priority == "INC" then 
         for i=1,queuecount do
            local queue = queueprefix .. i .. ":" .. queuename
            redis.call('zincrby', queue, -1, jobHash)
         end
      else
         for i=1,queuecount do
            local queue = queueprefix .. i .. ":" .. queuename
            redis.call('zadd', queue, tonumber(priority), jobHash)
         end
      end

      redis.call('hset', progress, jobHash, queuecount)
      redis.call('del', results)
      redis.call('publish', chann, jobName)
      ]] 
      return  script, 6, MRCHANNEL .. queue, MRJOBS .. queue, MRPROGRESS .. queue, MRWAITING .. queue, MRRESULTS .. queue .. ":" .. jobHash, MRCONFIG .. queue, jobJson, jobName, jobHash, priority, MRQUEUE, queue, cb

   end,

     
   mrreenqueue = function(queue, jobJson, jobName, jobHash, failureHash, priority, cb)
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
      return  script, 8, MRQUEUE .. queue, MRCHANNEL .. queue, MRJOBS .. queue, MRBUSY .. queue, MRWAITING .. queue, common.FAILED, common.FAILED_ERROR, common.FAILEDTIME, jobJson, jobName, jobHash, failureHash, priority, cb

   end,
     
   --DONE
   -- check for waiting jobs.  see if they're on the busy list.  if not, increment them on the queue
   -- note, could be more efficient by tallying up the times i see a hash and zincrby only once per hash
   -- but this is already pretty elaborate.  don't want more moving parts to get me confused right now
   -- take top job off queue and return it
   -- TODO: on the running list, set the queue to the subqueue so it will get cleaned properly on crash
   mrdequeue = function(queue, workername, cb)
      script = [[
         local reducequeue = KEYS[1]
         local jobs = KEYS[2]
         local progress = KEYS[3]
         local waiting = KEYS[4]
         local assignment = KEYS[5]
         local running = KEYS[6]
         local runningSince = KEYS[7]

         local workername = ARGV[1]
         local currenttime = ARGV[2] 
         local queueprefix = ARGV[3]
         local queuename = ARGV[4]
         local resultsPrefix  = ARGV[5]

         local waitingJobs = redis.call('smembers', waiting)

         if #waitingJobs > 0 then
            local waitingHashes = {}
            for i,job in ipairs(waitingJobs) do
               local shit, crap, jobHash = job:find('"hash":"(.-)"')
               table.insert(waitingHashes, jobHash)
            end

            local busyList = {}

            if #waitingHashes > 0 then
               busyList = redis.call('hmget', progress, unpack(waitingHashes))
            end

            local readyJobHashes = {}
            local readyJobs = {}
            local readyResults = {}

            for i,jobHash in ipairs(waitingHashes) do
               if not busyList[i] then
                  table.insert(readyJobHashes, jobHash)
                  table.insert(readyJobs, waitingJobs[i])
                  table.insert(readyResults, resultsPrefix .. queuename .. ":" .. jobHash)
                  redis.call('hset', jobs, jobHash, waitingJobs[i])
               end
            end

            if #readyJobs > 0 then 
               redis.call('srem', waiting, unpack(readyJobs))
               redis.call('hdel', progress, unpack(readyJobs)) 
               redis.call('del', results, unpack(readyResults))

               local queuecount = redis.call('hget', mrconfig, "nqueues")

               for i,jobHash in ipairs(waitingHashes) do
                  for j=1,queuecount do
                     local queue = queueprefix .. j .. ":" .. queuename
                     redis.call('zincrby', queue, -1, jobHash)
                  end
               end
            end
         end
  

         local isReduce = 1

         local topJobHash = redis.call('zrange', reducequeue, 0, 0)[1]
         redis.call('zremrangebyrank', reducequeue, 0, 0)

         if not topJobHash then
            isReduce = 0
            local myqueue = redis.call('hget', assignment, workername)
            topJobHash = redis.call('zrange', myqueue, 0, 0)[1]
            redis.call('zremrangebyrank', myqueue, 0, 0)
         end

         local topJob = nil

         if topJobHash then
            topJob = redis.call('hget', jobs, topJobHash)
            redis.call('hset', running, workername, topJob)
            redis.call('hset', runningSince, workername, currenttime)
         end

         if topJob then
            return {topJob, isReduce}
         else
            return nil
         end

      ]]

      return script, 7, MRQUEUE .. "0:" .. queue, MRJOBS .. queue, MRPROGRESS .. queue, MRWAITING .. queue, MRASSIGNMENT .. queue, common.RUNNING, common.RUNNINGSINCE, workername, os.time(), MRQUEUE, queue, MRRESULTS, cb
   end,
    -- this needs to be built out better 
   mrfailure = function(workername, queue, jobHash, errormessage, cb)
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

      return script, 4, common.RUNNING, common.FAILED, common.FAILED_ERROR, common.FAILEDTIME, workername, MRQUEUE .. queue, jobHash, errormessage, os.time(), cb
   end,
           
   --Done
   mrcleanup = function(queue, workername, jobHash, jobName, cb)

      -- job's done, take it off the running list, worker is no longer busy
      -- dont remove running time since we want to use that to say when the worker last had a job

      local script = [[
      local runningJobs = KEYS[1]
      local progress = KEYS[2]
      local results = KEYS[3]
      local assignment = KEYS[4]
      local waiting = KEYS[5]
      local jobs = KEYS[6]
      local reducequeue = KEYS[7]
      local chann = KEYS[8]

      local workername = ARGV[1]
      local jobHash = ARGV[2]
      local jobName = ARGV[3]

      redis.call('hdel', runningJobs, workername)
     
      local myqueue = redis.call('hget', assignment, workername)
      local x,y,myqueuenum = myqueue:find("MRQUEUE:(%d*)")

      redis.call('hset', results, myqueue, '{"result":"SUCCESS"}')
      local curprog = redis.call('hincrby', progress, jobHash, -1)

      if curprog == 0 then
         redis.call('zincrby', reducequeue, -1, jobHash)
         redis.call('publish', chann, jobName)
      elseif curprog == -1 then
         print("REDUCING")
         redis.call('hdel', progress, jobHash)
         redis.call('del', results)
         redis.call('hdel', jobs, jobHash)
         local waitingJobs = redis.call('smembers', waiting)

         for i,job in ipairs(waitingJobs) do
            local shit, crap, waitingJobHash = job:find('"hash":"(.-)"')
            if waitingJobHash == jobHash then
               redis.call('publish', chann, jobName)
               return 1
            end
         end
      end

      return 2

      ]]

      return script, 8, common.RUNNING, MRPROGRESS .. queue, MRRESULTS .. queue .. ":" .. jobHash, MRASSIGNMENT .. queue, MRWAITING .. queue, MRJOBS .. queue, MRQUEUE .. "0:" .. queue, MRCHANNEL .. queue, workername, jobHash, jobName, cb

   end,
     
}

local mrqueue = {}

function mrqueue.subscribe(queue, jobs, cb)

   --job list must have a config element that specifies worker number
   queue.environment.redis.eval(evals.startup(queue.name, queue.environment.workername, jobs.config.nodenum))

   async.fiber(function()
      for jobname, job in pairs(jobs) do
        
         if type(job) == 'table' and not job.skip then
            if job.prepare then
               wait(job.prepare, {})
            end
            queue.jobs[jobname] = job
         else
            queue.jobs[jobname] = {map = job, reduce = function() end}
         end
            
      end

      queue.environment.subscriber.subscribe(MRCHANNEL .. queue.name, function(message)
         queue.dequeueAndRun()
      end)

      queue.donesubscribing(cb)
   end)
end

function mrqueue.enqueue(queue, jobName, argtable, cb)

   -- instance allows multiple identical jobs to sit on the waiting set
   local job = { queue = MRQUEUE .. queue.name, name = jobName, args = argtable.jobArgs, instance = async.hrtime(),}
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
   job.priority = priority

   cb = cb or function(res) return end

   local jobJson = json.encode(job)
   queue.environment.redis.eval(evals.mrenqueue(queue.name, jobJson, jobName, jobHash, priority, cb))
end

function mrqueue.reenqueue(queue, argtable, cb)
   queue.environment.redis.eval(evals.mrreenqueue(argtable.queueName, argtable.jobJson, argtable.jobName, argtable.jobHash, argtable.failureId, INCREMENT, cb))
end

function mrqueue.dequeue(queue, cb)

   queue.environment.redis.eval(evals.mrdequeue(queue.name, queue.environment.workername, function(response) 
      if response and response[1] then
         local res = json.decode(response[1])
         res.isReduce = response[2] == 1
         print("RESPONSE",res)
         cb(res)
      else
         cb(nil)
      end
   end))
end

function mrqueue.failure(queue, argtable,cb)
   queue.environment.redis.eval(evals.mrfailure(queue.environment.workername, queue.name, argtable.jobHash, argtable.err, cb))
end

function mrqueue.cleanup(queue, argtable, cb)
   queue.environment.redis.eval(evals.mrcleanup(queue.name, queue.environment.workername, argtable.jobHash, argtable.jobName, function(res) print("CLEANUP", res) end))
end

local jobAndMethod = function(res)
   local name = res.name
   local method = "map"
   if name:find("FAILURE:") then
      method = "failure"
      local x,y,subname = name:find("FAILURE:(.*)")
      name = subname
   end
   return name, method
end

function mrqueue.doOverrides(queue)
   queue.execute = function(res)
      local name, method = jobAndMethod(res)
      local job = queue.jobs[name]
      if job == nil then
         log.print("ERROR -- no job found for jobname: " .. name .. " method: " .. method)
         log.print(res)
      end
      if job[method] then
         job[method](res.args)
      else
         log.print("received job " .. name .. " method " .. method .. ":  No such method for job")
      end
   end

   queue.failure = function(argtable, res)
      mrqueue.failure(queue, argtable)

      local name, method = jobAndMethod(res)
      local job = queue.jobs[name]

      if method == "run" and job.failure then
         mrqueue.enqueue(queue, "FAILURE:" .. name, {jobHash = argtable.jobHash, jobArgs = res.args, priority = res.priority})
      end

   end

end

return mrqueue
