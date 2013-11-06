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
         queue:enqueue("TEST", "testJob", {a = 1, b = "test", testnumber = i})
      end
      
      async.setTimeout(2800, function()
         client.del("QUEUE:TEST")
         client.del("UNIQUE:TEST")
      end)
      async.setTimeout(3000, function()
         client.close()
      end)

   end)
end)

async.go()



