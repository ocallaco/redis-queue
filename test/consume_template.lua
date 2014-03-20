local q = require 'redis-queue.init'
local rc = require 'redis-async'

local async = require 'async'
local fiber = require 'async.fiber'

local redis_addr = {host='127.0.0.1', port=6379}

local tester = {}


-- implement/override these to make your own tests
--
--
--

tester.nWorkers = 10

tester.prepareEnvironment = function()
end

tester.prepareWorker = function(worker)
end

-- must implement this!
tester.generateJob = function(worker) 
   error("GENERATEJOB NOT IMPLEMENTED")
end

tester.evaluateCode = function(i)
end

--
--
--
--



tester.run = function()

   fiber(function()

      tester.prepareEnvironment()
      for i = 1,tester.nWorkers do

         local worker = {}
         worker.index = i
         worker.name = "Worker" .. i
      
         tester.prepareWorker(worker)
         -- test must fill this job in!
         local jobDescriptor = tester.generateJob(worker)

         rc.connect(redis_addr, function(client)

            print("CONNECTED")
            q(client, function(newqueue)
               worker.queue = newqueue
               print(worker.name .. " is up")


               worker.queue:registerWorker(redis_addr, jobDescriptor, function()
                  print("test start " .. i)

                  async.setTimeout(15000, function()
                     tester.evaluateCode(i, client)
                  end)

                  async.setTimeout(16000, function()
                     print(worker.name .. " closing")
                     client.close()
                     worker.queue:close()


                  end)
               end)
            end)
         end)
      end
   end)

   async.go()
end

return tester
