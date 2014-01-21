local q = require 'redis-queue.init'
local rc = require 'redis-async'

local async = require 'async'
local fiber = require 'async.fiber'

fiber(function()

   local redis_client


   rc.connect({host='localhost', port=6379}, function(client)
      redis_client = client

      local queue 
      q(redis_client, function(newqueue)
         queue = newqueue
         print("test start")

         for i = 1,200 do
            queue:enqueue("WALL", "testJob", {conallhash  = "TEST", a = 1, b = "test", testnumber = i}, i)
         end
      end)
      
      async.setTimeout(2800, function()
--         client.del("QUEUE:WALL")
--         client.del("UNIQUE:WALL")
      end)
      async.setTimeout(3000, function()
         client.close()
      end)

   end)
end)

async.go()



