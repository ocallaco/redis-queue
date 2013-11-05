local q = require './init'
local rc = require 'redis-async'

local async = require 'async'
local fiber = require 'async.fiber'

fiber(function()

   local redis_client


   rc.connect({host='localhost', port=6379}, function(client)
      redis_client = client

      local queue = q(redis_client)

      print("test start")

      for i = 1,200 do
         local rnd = torch.uniform(0,1)
         if rnd > 0.5 then
            queue:lbenqueue("TEST", "testJob", {a = 1, b = "test", testnumber = (i % 25) + 1 }, tostring((i % 25) + 1))
         end
      end
      
      async.setTimeout(7000, function()
         
         --client.del("LBQUEUE:TEST")
         --client.del("LBWAITING:TEST")
         --client.del("LBBUSY:TEST")
         --client.del("LBJOBS:TEST")
         client.close()
      end)

   end)
end)

async.go()



