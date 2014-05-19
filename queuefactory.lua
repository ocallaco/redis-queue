local json = require 'cjson'
local async = require 'async'

local queuefactory = {}

local ILLEGAL_ARGS = {
   "name",
   "instance",
   "queue",
   "args",
   "hash",
}

local checkArgs = function(args)
   argsjson = json.encode(args)

   for i,arg in ipairs(ILLEGAL_ARGS) do
      local key = argsjson:find('"' .. arg .. '":')
      if key then
         error("Illegal string in arguments table: " .. arg)
      end
   end
end

function queuefactory:init(environment)
   self.environment = environment
end

function queuefactory:newqueue(name, queueType)
   if not self.environment then
      error("Cannot create queues until queue factory is properly initiated")
   end
   local queue = {}

   queue.name = name
   queue.jobs = {}
   queue.type = queueType.type
   queue.environment = self.environment

   local redis = self.environment.redis

   queue.startup = function(cb)
      queueType.startup(queue, cb)
   end

   queue.subscribe = function(jobs, cb)   
      queue.state = "Subscribing"
      if queue.environment == nil or queue.environment.subscriber == nil then
         error('Environment not ready for subscription -- either not inited or subscription connection not opened yet')
      end

      queueType.subscribe(queue, jobs, cb)
   end

   queue.donesubscribing = function(cb)
      queue.waiting = true
      queue.dequeueAndRun()
      queue.state = "Ready"
      if cb then cb() end
   end

   queue.enqueue = function(jobName, args, cb)
      local jobArgs = args.jobArgs
      checkArgs(jobArgs)

      local callback = cb
      if self.environment.enqueueTimeout ~= 0 then
         local enqueuejobtimeout = async.setTimeout(self.environment.enqueueTimeout or 60000, function() 
            local error_string = "Redis Queue Timed out enqueueing:\nJobName: " ..  tostring(jobName) .. "\nQueueName: " .. queue.name
--            error(error_string)
            print(error_string) -- not erroring until we have things stable
         end)

         callback = function(res)
            enqueuejobtimeout:clear()
            if cb then 
               cb(res)
            end
         end
      end
      
      queueType.enqueue(queue, jobName, args, callback)
   end

   queue.reenqueue = function(failureId, jobJson, cb)
      
      local _,_,jobHash = jobJson:find('"hash":"(.-)"')
      local _,_,jobName = jobJson:find('"name":"(.-)"')

      queueType.reenqueue(queue, {queueName = queue.name, jobJson = jobJson, jobName = jobName, jobHash = jobHash, failureId = failureId},cb)
   end

   queue.dequeueAndRun = function()

      -- atomically pop the job and push it onto an hset

      -- if the worker is busy, set a reminder to check that queue when done processing, otherwise, process it
      if queue.busy or queue.environment == nil or queue.environment.workername == nil then
         queue.waiting = true
         return
      end

      -- need to set this before pulling a job off the queue to ensure one job at a time
      queue.busy = true

      queue.workerstate = "Dequeuing job"

      queueType.dequeue(queue, function(res)

         if res then
            async.pcall(async.fiber(function()
               queue.state = "Running:" .. res.name
               
               local jobresult
               local jobsuccess = true

               xpcall(function()
                  jobresult = queue.execute(res)
               end,
               function(er)
                  jobsuccess = false
                  local err = debug.traceback(er)
                  print(err) 
                  local jobHash
                  if not res.hash or res.hash == "0" then
                     jobHash = res.name .. ":" .. json.encode(res.args)
                  else
                     jobHash = res.hash
                  end

                  queue.failure({jobHash=jobHash, err=err}, res)     
               end)

               queue.cleanup({response = res, jobresult = jobresult, jobHash = res.hash, jobName = res.name, success = jobsuccess}, res)
 
               queue.state = "Ready"
               queue.busy = false

               -- job's completed, let's check for other jobs we might have missed
               if queue.waiting then 
                  queue.dequeueAndRun()
               end
            end))
         else
            queue.busy = false
            queue.waiting = false
            queue.state = "Ready"
         end
      end)
   end

   queue.execute = function(res)
      local job = queue.jobs[res.name]
      return job(res.args)
   end

   queue.failure = function(argtable, res)
      queueType.failure(queue, argtable)
   end

   queue.cleanup = function(argtable, res)
      queueType.cleanup(queue, argtable)
   end

   if queueType.doOverrides then
      queueType.doOverrides(queue)
   end

   return queue

end

return queuefactory
