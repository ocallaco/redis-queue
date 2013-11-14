local q = require './init'
local rc = require 'redis-async'

local async = require 'async'
local fiber = require 'async.fiber'

local redis_addr = {host='localhost', port=6379}

local errfunc2 = function(x)
   assert(x)
end
local errfunc = function()
   errfunc2(false)
end

fiber(function()

   local jobsSeen = {}
   local jobsSeenBy = {}

   for i = 1,10 do 
      local worker = {}
      table.insert(jobsSeenBy, {})

      worker.name = "Worker" .. i
  
      rc.connect(redis_addr, function(client)

         print(worker.name .. " is up")

         worker.queue = q(client)
         worker.queue:registerWorker(redis_addr, function()
            print("test start " .. i)
            worker.queue:subscribeJob("WALL", "testJob", function(args) 
               errfunc()
            end)
         end)
         

         if i == 1 then
            async.setTimeout(15000, function()
               -- check no jobs in running list or uniqueness hash
               client.hlen("RESERVED:RUNNINGJOBS", function(res)
                  print("RUNNINGJOBS is " .. res .. " should be 0")
               end)

               client.hlen("UNIQUE:USER", function(res)
                  print("UNIQUE is " .. res .. " should be 0")
               end)
            end)
         end
         async.setTimeout(17000, function()
            client.close()
            worker.queue:close()
         end)
      end)
   end


end)

async.go()



