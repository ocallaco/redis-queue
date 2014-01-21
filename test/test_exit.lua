local q = require './init'
local rc = require 'redis-async'

local async = require 'async'
local fiber = require 'async.fiber'

local redis_addr = {host='localhost', port=6379}

fiber(function()

   local jobsSeen = {}
   local jobsSeenBy = {}

   local worker = {}
   table.insert(jobsSeenBy, {})

   worker.name = "Worker" 

   rc.connect(redis_addr, function(client)

      print(worker.name .. " is up")

      worker.queue = q(client)
      worker.queue:registerWorker(redis_addr, function()
         print("test start ")
         worker.queue:subscribeLBJob("USER", "testJob", function(args) 
            os.exit()
         end)

         worker.queue:doneSubscribing()
      end)

      async.setTimeout(15000, function()
         client.close()
         worker.queue:close()
      end)
   end)


end)

async.go()



