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
  
      rc.connect(redis_addr, function(client)

         print(worker.name .. " is up")

         worker.queue = q(client)
         worker.queue:registerWorker(redis_addr, function()
            print("test start " .. i)
            worker.queue:subcribeJob("TEST", "testJob", function(args) 
               if jobsSeen[args.testnumber] then
                  error("JOB " .. args.testnumber .. " already seen!")
               end
               jobsSeen[args.testnumber] = true
               table.insert(jobsSeenBy[i], args.testnumber)
            end)
         end)
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
   end
end)

async.go()



