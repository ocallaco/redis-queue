local tester = require './consume_template'

tester.prepareEnvironment = function()
   tester.jobsSeenBy = {}
   tester.jobsSeen = {}
end

tester.prepareWorker = function(worker)
   tester.jobsSeenBy[worker.index] = {}
end

tester.generateJob = function(worker)
   local i = worker.index
   local jobDescriptor = {WALL = {
      testJob = function(args) 
         if tester.jobsSeen[args.testnumber] then
            error("JOB " .. args.testnumber .. " already seen!")
         end
         tester.jobsSeen[args.testnumber] = true
         table.insert(tester.jobsSeenBy[i], args.testnumber)
      end}
   }
   return jobDescriptor
end

tester.evaluateCode = function(i)
   print("jobs seen by worker " .. i .. ": " .. #tester.jobsSeenBy[i])

   if i == 1 then
      for j = 1,200 do
         if not tester.jobsSeen[j] then
            print("!!!!!!!!!!!!Missed job " .. j)
         end
      end
   end
end

tester.run()
