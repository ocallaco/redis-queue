local tester = require './consume_template'

tester.eval_timeout = 30000

tester.prepareEnvironment = function()
   tester.jobsSeenBy = {}
   tester.jobsSeen = {}
end

tester.prepareWorker = function(worker)
   tester.jobsSeenBy[worker.index] = {}
end

tester.generateJob = function(worker)
   local i = worker.index
   local jobDescriptor = {
      INSTAGRAM = {
         testJob = {
            run = function(args) 
               local jobtime = Date(args['time'])
               print("Worker " .. worker.name .. " received ", args)

               local currrettime = os.time()
               print("current time " .. (args['time'] - currrettime))

               tester.jobsSeen[args.testnumber] = true
               if args.testnumber == 45 then
                  --os.exit()
               end

               table.insert(tester.jobsSeenBy[i], args.testnumber)
            end,

            failure = function(args)
               print("FAILURE", args)
            end
         }
      }
   }
   return jobDescriptor
end

tester.evaluateCode = function(i)
   print("jobs seen by worker " .. i .. ": " .. #tester.jobsSeenBy[i])

   if i == 1 then
      for j = 1,45 do
         if not tester.jobsSeen[j] then
            print("!!!!!!!!!!!!Missed job " .. j)
         end
      end
   end
end

tester.run()
