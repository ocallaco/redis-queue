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

         queue:enqueueJob("USER", "testJob", {a = 1, b = "test", testnumber = (25) + 1 }, {jobHash = tostring((25) + 1)})
         print("ENQUEUED")
               
         async.setTimeout(15000, function()
            queue:enqueueJob("USER", "testJob", {a = 1, b = "test", testnumber = (25) + 1 }, {jobHash = tostring((25) + 1)})
            print("ENQUEUED2")

            --client.del("LBQUEUE:TEST")
            --client.del("LBWAITING:TEST")
            --client.del("LBBUSY:TEST")
            --client.del("LBJOBS:TEST")
         end)
      end)


   end)
end)

async.go()



