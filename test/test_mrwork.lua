local tester = require './consume_template'

tester.prepareEnvironment = function()
   tester.jobsSeenBy = {}
   tester.jobsSeen = {}
   tester.nWorkers = 4
end

tester.prepareWorker = function(worker)
   tester.jobsSeenBy[worker.index] = {}
end

tester.generateJob = function(worker)
   local i = worker.index
   local jobDescriptor = {MRSIM = {
      testJob = {
         map= function(args) 
            print("MAP",pretty.write(args))
            tester.jobsSeen[args.testnumber] = true
            table.insert(tester.jobsSeenBy[i], args.testnumber)
         end,
         reduce = function(args) 
            print("REDUCE", pretty.write(args))   
         end,
      },
      config = {skip = true, nodenum = ((i % 4) + 1)}
   }}

   print("NODE NUM", ((i % 4) + 1))

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
