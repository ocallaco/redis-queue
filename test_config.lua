local q = require './init'
local rc = require 'redis-async'

local async = require 'async'
local fiber = require 'async.fiber'

fiber(function()

   local redis_client




   rc.connect({host='localhost', port=6379}, function(client)
      redis_client = client

      local queue = q(client)

      print("test start")

      local testConfig = {
         ['TEST1'] = "QUEUE",
         ['TEST2'] = "LBQUEUE",
         ['TEST3'] = "DELQUEUE",
      }
      queue.config:setConfig(testConfig)

      async.setTimeout(2800, function()

         queue.config:fetchConfig()

      end)
      async.setTimeout(3000, function()
         print(pretty.write(queue.config.configtbl))
         client.close()
      end)

   end)
end)

async.go()
