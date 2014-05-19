local q = require 'redis-queue.init'
local mrqueue = require '../../queues/mrqueue'
local rc = require 'redis-async'

local async = require 'async'
local fiber = require 'async.fiber'

local redis_addr = {host='127.0.0.1', port=6379}

fiber(function()
   rc.connect(redis_addr, function(client)
      print("CONNECTED")
      q(client, function(newqueue)
         fiber(function()
            name = "MRSIM"
            print(mrqueue.show(client, name))
         end)
      end)
   end)
end)

async.go()
