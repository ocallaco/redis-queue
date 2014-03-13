local q = require 'redis-queue'
local rc = require 'redis-async'

local async = require 'async'
local fiber = require 'async.fiber'

fiber(function()

   local redis_client


   rc.connect({host='127.0.0.1', port=6379}, function(client)
      redis_client = client

      q(redis_client, function(newqueue)
         local queue = newqueue

         print("test start")

         for i = 1,25 do
            local rnd = torch.uniform(0,1)
            if rnd > 0.5 then
               queue:enqueueJob("MRSIM", "testJob", {a = 1, b = "test", testnumber = (i % 25) + 1 }, {jobHash = tostring((i % 25) + 1)}, function(res) print(res)end)
            end
         end
      end)
      
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



