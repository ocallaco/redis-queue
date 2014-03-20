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
local MRWAITING = "MRWAITING:" -- Hash jobHash => jobJson

   -- other constants
local INCREMENT = "INC"


local evals = {

   --TODO: make it enqueue reduce step if progress comes down to 0, make it take job off waiting list if it was a reduce
   -- DONE
   startup = function(queue, workername, nodenum, cb)
      local script = [[
         local jobmatch = KEYS[1]
         local assignment = KEYS[2]
         local mrconfig = KEYS[3]
         local progress = KEYS[4]
         local waiting = KEYS[5]
         local chann = KEYS[6]


         local cleanupPrefix= ARGV[1]
         local queueprefix = ARGV[2]
         local queuename = ARGV[3]
         local workername = ARGV[4]
         local nodenum = tonumber(ARGV[5])
         local resultsPrefix = ARGV[6]
         
         local queuecount = redis.call('hget', mrconfig, "nqueues")
         local reducequeue = queueprefix .. "0:" .. queuename

         for j=1,queuecount do 
            local subqueuename = queueprefix .. j .. ":" .. queuename
            local cleanupName = cleanupPrefix .. subqueuename

            local cleanupJobs = redis.call('lrange', cleanupName, 0, -1)

            for i,cleanupJob in ipairs(cleanupJobs)do
               local x,y,jobHash = cleanupJob:find('.*"hash":"(.-)".*')
               local remaining = redis.call('hincrby', progress, jobHash, -1)

               local z,w,jobName = cleanupJob:find('.*"name":"(.-)".*')

               local results = resultsPrefix .. jobHash

               redis.call('hset', results, subqueuename, '{"result":"FAILURE","info":{"reason":"DEADWORKER"}}')

               if remaining == 0 then
                  redis.call('zincrby', reducequeue, -1, jobHash)
                  redis.call('publish', chann, jobName)
               end
            end
            redis.call('del', cleanupName)
         end


         local cleanupName = cleanupPrefix .. queueprefix .. "0:" .. queuename
         local cleanupJobs = redis.call('lrange', cleanupName, 0, -1)

         for i,cleanupJob in ipairs(cleanupJobs) do
            local x,y,jobHash = cleanupJob:find('.*"hash":"(.-)".*')

            local waitingJob = redis.call('hget', waiting, jobHash)

            if waitingJob then
               redis.call('hset', progress, jobHash, queuecount)
               redis.call('hset', jobmatch, jobHash, waitingJob)
               redis.call('hdel', waiting, jobHash)
               for i=1,queuecount do
                  local queue = queueprefix .. i .. ":" .. queuename
                  redis.call('zincrby', queue, -1, jobHash)
               end
            else
               redis.call('hdel', jobmatch, jobHash)
               redis.call('hdel', progress, jobHash)
            end
         end
         redis.call('del', cleanupName)


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
         local allWorkers = redis.call('hkeys', assignment)
         for i,worker in ipairs(allWorkers) do
            if not liveWorkers[worker] then
               table.insert(deadWorkers, worker)
            end
         end

         if #deadWorkers > 0 then
            redis.call('hdel', assignment, unpack(deadWorkers))
         end

         redis.call('hset', assignment, workername, queueprefix .. nodenum .. ":" .. queuename)
      ]]
      return script, 6, MRJOBS .. queue, MRASSIGNMENT .. queue, MRCONFIG .. queue, MRPROGRESS .. queue, MRWAITING .. queue, MRCHANNEL .. queue, common.CLEANUP, MRQUEUE, queue, workername, nodenum, MRRESULTS .. queue .. ":", cb
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

      local jobExists = redis.call('hget', jobmatch, jobHash)

      if jobExists then
         local progcount = redis.call('hget', progress, jobHash) 
         if progcount and tonumber(progcount) >= 0 then
            redis.call('hset', waiting, jobHash, jobJson)
            redis.call('publish', chann, jobName)
            return
         end
      else
         redis.call('hset', jobmatch, jobHash, jobJson)
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
         local reducequeue = KEYS[1] --
         local jobs = KEYS[2] --
         local assignment = KEYS[3] --
         local running = KEYS[4] --
         local runningSince = KEYS[5] --

         local queuename = ARGV[1]
         local resultsPrefix = ARGV[2]
         local workername = ARGV[3] --
         local currenttime = ARGV[4] --

         local isReduce = 1

         local myqueue = reducequeue
         local topJobHash = redis.call('zrange', reducequeue, 0, 0)[1]
         redis.call('zremrangebyrank', reducequeue, 0, 0)

         if not topJobHash then
            isReduce = 0
            myqueue = redis.call('hget', assignment, workername)
            topJobHash = redis.call('zrange', myqueue, 0, 0)[1]
            redis.call('zremrangebyrank', myqueue, 0, 0)
         end

         local topJob = nil

         if topJobHash then
            topJob = redis.call('hget', jobs, topJobHash):gsub('"' .. queuename .. '"', '"' .. myqueue .. '"')
            redis.call('hset', running, workername, topJob)
            redis.call('hset', runningSince, workername, currenttime)
         end

         if topJob then
            return {topJob, (isReduce == 1) and redis.call('hgetall', resultsPrefix .. topJobHash)}
         else
            return nil
         end

      ]]

      return script, 5, MRQUEUE .. "0:" .. queue, MRJOBS .. queue, MRASSIGNMENT .. queue, common.RUNNING, common.RUNNINGSINCE,MRQUEUE .. queue, MRRESULTS .. queue .. ":", workername, os.time(), cb
   end,
    -- this needs to be built out better 
    -- IN PROGRESS -- written but not tested
    -- does decrement and enqueues reduce, but doesn't enqueue waiting jobs (leaves that to cleanup)
   mrfailure = function(workername, queue, jobHash, errormessage, subqueue, cb)
      local script = [[
      local workername = ARGV[1]
      local jobHash = ARGV[2]
      local errormessage = ARGV[3]
      local currenttime = ARGV[4]
      local queue = ARGV[5]

      local runningJobs = KEYS[1]
      local failedJobs = KEYS[2]
      local failureReasons = KEYS[3]
      local failureTimes = KEYS[4]
      local progress = KEYS[5]
      local chann = KEYS[6]
      local assignment = KEYS[7]
      local results = KEYS[8]
      local reducequeue = KEYS[9]

      local progcount = redis.call('hincrby', progress, jobHash, -1) 
      
      local job = redis.call('hget', runningJobs, workername)

      local failureHash = queue .. ":" .. jobHash
      
      if progcount and tonumber(progcount) == -1 then
         redis.call('hset', failedJobs, failureHash, job)
         redis.call('hset', failureReasons, failureHash, errormessage)
         redis.call('zadd', failureTimes, 0 - currenttime, failureHash)
      else
         local subqueuename = redis.call('hget', assignment, workername)
         redis.call('hset', results,  subqueuename, '{"result":"FAILURE","info":{"reason":"' .. errormessage .. '"}}')
            
         if progcount and tonumber(progcount) == 0 then
            redis.call('zincrby', reducequeue, -1, jobHash)
            redis.call('publish', chann, "REDUCE")
         end
      end

      return redis.call('hdel', runningJobs, workername)


      ]]

      return script, 9, common.RUNNING, common.FAILED, common.FAILED_ERROR, common.FAILEDTIME, MRPROGRESS .. queue, MRCHANNEL .. queue, MRASSIGNMENT .. queue, MRRESULTS .. queue .. ":" .. jobHash, MRQUEUE .. "0:" .. queue, workername, jobHash, errormessage, os.time(), subqueue, cb
   end,
           
   --NOTE: failure on reduce seems to leave it in a bad state, where waiting jobs don't get added back...
   mrcleanup = function(queue, workername, jobHash, jobName, success, cb)

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
      local mrconfig = KEYS[9]

      local workername = ARGV[1]
      local jobHash = ARGV[2]
      local jobName = ARGV[3]
      local queueprefix = ARGV[4]
      local queuename = ARGV[5]
      local success = tonumber(ARGV[6]) == 1

         
      redis.call('hdel', runningJobs, workername)
     
      local myqueue = redis.call('hget', assignment, workername)
      local x,y,myqueuenum = myqueue:find("MRQUEUE:(%d*)")

      local curprog

      if success then
         redis.call('hset', results, myqueue, '{"result":"SUCCESS"}')
         curprog = redis.call('hincrby', progress, jobHash, -1)
      else 
         curprog = tonumber(redis.call('hget', progress, jobHash))
      end


      if curprog == 0 and success then
         redis.call('zincrby', reducequeue, -1, jobHash)
         redis.call('publish', chann, jobName)
      elseif curprog == -1 then
         redis.call('del', results)
         local waitingJob = redis.call('hget', waiting, jobHash)

         if waitingJob then
            local queuecount = redis.call('hget', mrconfig, "nqueues")
            for i=1,queuecount do
               local queue = queueprefix .. i .. ":" .. queuename
               redis.call('zincrby', queue, -1, jobHash)
            end
            redis.call('hset', jobs, jobHash, waitingJob)
            redis.call('hset', progress, jobHash, queuecount)
            redis.call('hdel', waiting, jobHash)
            redis.call('publish', chann, jobName)
         else
            redis.call('hdel', progress, jobHash)
            redis.call('hdel', jobs, jobHash)
         end
      end

      return 2

      ]]

      return script, 9, common.RUNNING, MRPROGRESS .. queue, MRRESULTS .. queue .. ":" .. jobHash, MRASSIGNMENT .. queue, MRWAITING .. queue, MRJOBS .. queue, MRQUEUE .. "0:" .. queue, MRCHANNEL .. queue, MRCONFIG .. queue, workername, jobHash, jobName, MRQUEUE, queue, (success and 1) or 0, cb

   end,
     
}

local mrqueue = {}

function mrqueue.subscribe(queue, jobs, cb)

   --job list must have a config element that specifies worker number
   queue.environment.redis.eval(evals.startup(queue.name, queue.environment.workername, jobs.config.nodenum, function(res) print("STARTUP", res)end))
   --queue.environment.redis.eval(evals.startup(queue.name, queue.environment.workername, jobs.config.nodenum, cb))

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
      --print("DEQUEUE", response)
      if response and response[1] then
         local res = json.decode(response[1])
         if response[2] then
            res.isReduce = true
            local reduceResults = {}
            for i = 1,#response[2],2 do
               table.insert(reduceResults, response[2][i+1])
            end
            res.results = reduceResults
         end
         cb(res)
      else
         cb(nil)
      end
   end))
end

function mrqueue.failure(queue, argtable,cb)
   queue.environment.redis.eval(evals.mrfailure(queue.environment.workername, queue.name, argtable.jobHash, argtable.err, argtable.subqueue, cb))
   --queue.environment.redis.eval(evals.mrfailure(queue.environment.workername, queue.name, argtable.jobHash, argtable.err, argtable.subqueue, function(res) print("FAILURE", res)end))
end

function mrqueue.cleanup(queue, argtable, cb)
   --queue.environment.redis.eval(evals.mrcleanup(queue.name, queue.environment.workername, argtable.jobHash, argtable.jobName, argtable.success, cb))
   queue.environment.redis.eval(evals.mrcleanup(queue.name, queue.environment.workername, argtable.jobHash, argtable.jobName, argtable.success, function(res) print("CLEANUP", res) end))
end

local jobAndMethod = function(res)
   local name = res.name
   local method = (res.isReduce and "reduce") or "map"
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
         if method == "reduce" then
            job[method](res.args, res.results)
         else
            job[method](res.args)
         end
      else
         log.print("received job " .. name .. " method " .. method .. ":  No such method for job")
      end
   end

   queue.failure = function(argtable, res)
      local subqueue = res.queue
      argtable['subqueue'] = subqueue
      mrqueue.failure(queue, argtable)

      local name, method = jobAndMethod(res)
      local job = queue.jobs[name]

      if method == "run" and job.failure then
         mrqueue.enqueue(queue, "FAILURE:" .. name, {jobHash = argtable.jobHash, jobArgs = res.args, priority = res.priority, subqueue = subqueue})
      end

   end

end



function mrqueue.show(redis, name)
   
   local queueNames = {}
   local queueSizes = {}

   local res = async.fiber.wait({redis.hgetall, redis.hgetall, redis.hlen, redis.hgetall}, {{MRCONFIG .. name},{MRPROGRESS .. name},{MRWAITING .. name},{MRASSIGNMENT .. name}})

   local config, progress, waiting, assignment
   config = res[1][1]
   progress_ar = res[2][1]
   waiting = res[3][1]
   assignment_ar = res[4][1]

   local nqueues = config[2]
   local getters = {}
   local args = {}
   local toprow = {"<tr>"}

   -- Make the queue sizes and waiting list size table
   for i=0,nqueues do 
      local queuename = MRQUEUE .. i .. ":" .. name
      table.insert(getters, redis.zcard)
      table.insert(args, {queuename})
      table.insert(toprow, "<th>".. queuename .."</th>")
   end

   table.insert(toprow, "<th>WAITING</th>")
   table.insert(toprow, "<th>WORKERS</th></tr>")

   res = async.fiber.wait(getters, args)

   local datarow = {"<tr>"}
   for i=0,nqueues do 
      table.insert(datarow, "<td>".. res[i+1][1] .."</td>")
   end

   table.insert(datarow, "<td>".. waiting .."</td><td>" .. #assignment_ar / 2 .. "</td></tr>")

   local queuetable = "<table>" .. table.concat(toprow) .. table.concat(datarow) .. "</table>"

   -- show job progress and results so far
   local jobs = {}
   local progress = {}

   for i=1,#progress_ar,2 do
      table.insert(jobs, progress_ar[i])
      progress[progress_ar[i]] = progress_ar[i+1]
   end


   local job_results_getter = {}
   local job_results_args = {}

   local top_entry = {"<table><tr><th>Job</th><th>Progress</th>"}
   for i=1,nqueues do 
      table.insert(top_entry,"<th>" ..  MRQUEUE .. i .. ":" .. name .."</th>")
   end
   table.insert(top_entry,"</tr>")
   

   for i,job in ipairs(jobs) do
      table.insert(job_results_getter, redis.hgetall)
      table.insert(job_results_args, {MRRESULTS .. name .. ":" .. job})
   end

   if #jobs > 0 then
      res = async.fiber.wait(job_results_getter, job_results_args)
   else
      res = {}
   end

   local results = {}
   for i,jobHash in ipairs(jobs) do
      local results_entry = {}
      for j=1,#res[i][1],2 do
         results_entry[res[i][1][j]] = res[i][1][j+1]
      end
      results[jobHash] = results_entry
   end

   local job_table = {table.concat(top_entry)}

   for i,jobHash in ipairs(jobs) do
      local row_entry = {}
      table.insert(row_entry, "<tr><td>" .. jobHash .. "</td><td>" .. progress[jobHash] .. "</td>")
      for j=1,nqueues do
         local qname = MRQUEUE .. j .. ":" .. name 
         table.insert(row_entry, "<td>" .. (results[jobHash][qname] or "PENDING" ).. "</td>")
      end
      table.insert(row_entry,"</tr>")
      table.insert(job_table, table.concat(row_entry))
   end
  
   table.insert(job_table, "</table>")
 
   job_table = table.concat(job_table)
   return queuetable  .. job_table

end

return mrqueue
