local q = require 'redis-queue'
local rc = require 'redis-async'

local async = require 'async'
local fiber = require 'async.fiber'

fiber(function()

   local redis_client

   rc.connect({host='localhost', port=6379}, function(client)
      redis_client = client

      local queue = q(redis_client)

      print("test start")
      queue:addJob("TEST", "testJob", {a = 1, b = "test", test = "last argument"})
--      queue:getJob("TEST", function(res) print(pretty.write(res)) end)
      print("done")
   end)
end)

async.go()



