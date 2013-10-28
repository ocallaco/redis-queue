json = require 'cjson'

RedisQueue = {meta = {}, test = "TEST"}

function RedisQueue.meta:__index(key)
   return RedisQueue[key]
end

function RedisQueue:addJob(queue, jobname, argtable)
   local job = { name = jobname, args = argtable}
   local jobJson = json.encode(job)

   print("JSON: " .. jobJson)
   self.redis.sadd("QUEUE:"..queue, jobJson)
end

function RedisQueue:getJob(queue, cb)
   self.redis.spop("QUEUE:"..queue, function(res)
      res = json.decode(res)
      cb(res)
   end)
end

function RedisQueue:new(redis, ...)
   local newqueue = {}
   newqueue.redis = redis
   setmetatable(newqueue, RedisQueue.meta)
   
   return newqueue
end

local queue = {}

setmetatable(queue, {
   __call = RedisQueue.new,
})

return queue

