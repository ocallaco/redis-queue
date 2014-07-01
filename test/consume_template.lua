local q = require 'redis-queue.init'
local rc = require 'redis-async'

--rc.setdebug()

local async = require 'async'
local fiber = require 'async.fiber'

local redis_addr = {host='127.0.0.1', port=6379}

-- dont print out the status messages
local status = require 'redis-status.api'
status.writeStatus = function() end

local tester = {}


-- implement/override these to make your own tests
--
--
--

tester.nWorkers = 10
tester.eval_timeout = 15000

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

                  if tester.eval_timeout and tester.eval_timeout > 0 then
                     async.setTimeout(tester.eval_timeout, function()
                        tester.evaluateCode(i, client)
                     end)

                     async.setTimeout(tester.eval_timeout + 1000, function()
                        print(worker.name .. " closing")
                        worker.queue:close()
                        client.close()
                     end)
                  else
                     evaluate = function()
                        tester.evaluateCode(i, client)
                     end
                     die = function()
                        print(worker.name .. " closing")
                        client.close()
                        worker.queue:close()
                     end
                  end
                  
               end)
            end)
         end)
      end
   end)

   async.go()
end

return tester
