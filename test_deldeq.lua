local q = require './init'
local rc = require 'redis-async'

local async = require 'async'
local fiber = require 'async.fiber'

local redis_addr = {host='localhost', port=6379}

io.stdout:setvbuf("no")

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
            worker.queue:subscribeDELJob("TAG", "testJob", function(args) 

               local jobtime = Date(args['time'])
               print("Worker " .. worker.name .. " received " .. tostring(jobtime))
               
               local currrettime = os.time()
               print("current time " .. (args['time'] - currrettime))



--               os.exit()
--               error("ERROR")

               jobsSeen[args.testnumber] = true
               table.insert(jobsSeenBy[i], args.testnumber)
            end)

            worker.queue:doneSubscribing()
         end)
         async.setTimeout(30000, function()

            -- check to see that all fields hold proper values

--            client.del("LBQUEUE:TEST")
--            client.del("LBWAITING:TEST")
--            client.del("LBBUSY:TEST")
--            client.del("LBJOBS:TEST")

            print(worker.name .. " closing")
            client.close()
            worker.queue:close()

            print("jobs seen by worker " .. i .. ": " .. #jobsSeenBy[i])

            if i == 1 then
               for j = 1,25 do
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





