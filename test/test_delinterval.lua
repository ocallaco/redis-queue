local q = require 'redis-queue'
local rc = require 'redis-async'

local async = require 'async'
local fiber = require 'async.fiber'

fiber(function()

   local redis_client


   rc.connect({host='localhost', port=6379}, function(client)
      redis_client = client

      q(redis_client, function(newqueue)
         local queue = newqueue

         print("test start")

         queue:enqueueJob("INSTAGRAM", "testJob", {a = 1, b = "test", testnumber = 1, time = os.time() +  1 }, {interval = 2, jobHash = 1, timestamp = os.time() +  1}, function(res) print(res) end)
         queue:enqueueJob("INSTAGRAM", "testJob", {a = 1, b = "test", testnumber = 2, time = os.time() +  1 }, {interval = 5, jobHash = 2, timestamp = os.time() + 1 }, function(res) print(res) end)
      end)
      
      async.setTimeout(8000, function()
         
         --client.del("DELQUEUE:TAG")
         --client.del("DELJOBS:TAG")
         client.close()
      end)

   end)
end)

async.go()



