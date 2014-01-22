local tester = require './consume_template'
local async = require 'async'

tester.prepareEnvironment = function()
   tester.jobsSeenBy = {}
   tester.jobsSeen = {}
end

tester.prepareWorker = function(worker)
   tester.jobsSeenBy[worker.index] = {}
end

tester.generateJob = function(worker)
   local i = worker.index
   local jobDescriptor = {USER = {
      testJob = {
         prepare = function(cb) 
            tester.prepared = "PREPARED" 
            async.setTimeout(1, function()
               cb()
            end)
         end,

         run = function(args) 
            os.exit()            
         end,

         failure = function(args)
            print("FAILURE RECOVERED", args.testnumber)
            tester.jobsSeen[args.testnumber] = true
            table.insert(tester.jobsSeenBy[i], args.testnumber)
            return false
         end
      }


      }
   }
   return jobDescriptor
end

tester.evaluateCode = function(i)
   print("jobs seen by worker " .. i .. ": " .. #tester.jobsSeenBy[i])

   if i == 1 then
      for j = 1,25 do
         if not tester.jobsSeen[j] then
            print("!!!!!!!!!!!!Missed job " .. j)
         end
      end
   end
end

tester.run()
