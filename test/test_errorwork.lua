local q = require './init'
local rc = require 'redis-async'

local async = require 'async'
local fiber = require 'async.fiber'

local redis_addr = {host='localhost', port=6379}



fiber(function()

   local jobsSeen = {}
   local jobsSeenBy = {}

   for i = 1,10 do
      
      local worker = {}
      table.insert(jobsSeenBy, {})

      worker.name = "Worker" .. i

      local jobDescriptor = {WALL = {
         testJob = function(args) 
            error("ERROR") 
         end}
      }

      rc.connect(redis_addr, function(client)

         print("CONNECTED")
         q(client, function(newqueue)
            worker.queue = newqueue
            print(worker.name .. " is up")
            worker.queue:registerWorker(redis_addr, jobDescriptor, function()
               print("test start " .. i)

               async.setTimeout(15000, function()
                  print(worker.name .. " closing")
                  client.close()
                  worker.queue:close()

                  print("jobs seen by worker " .. i .. ": " .. #jobsSeenBy[i])

                  if i == 1 then
                     for j = 1,200 do
                        if not jobsSeen[j] then
                           print("!!!!!!!!!!!!Missed job " .. j)
                        end
                     end
                  end
               end)
            end)
         end)
      end)
   end
end)

async.go()

