local q = require './init'
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

         for i = 25,1,-1 do
            queue:delenqueue("TAG", "testJob", {a = 1, b = "test", testnumber = i, time = os.time() + (i * 1) }, i, os.time() + (i * 1))
            queue:delenqueue("TAG", "testJob", {a = 1, b = "test", testnumber = i + 25, time = os.time() + (i * 1) }, i + 25, os.time() + (i * 1))
            queue:delenqueue("TAG", "testJob", {a = 1, b = "test", testnumber = i + 50, time = os.time() + (i * 1) }, i + 50, os.time() + (i * 1))
         end
      end)
      
      async.setTimeout(7000, function()
         
         --client.del("DELQUEUE:TAG")
         --client.del("DELJOBS:TAG")
         client.close()
      end)

   end)
end)

async.go()



