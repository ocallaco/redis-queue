local tester = require '../consume_template'

tester.prepareEnvironment = function()
   tester.jobsSeenBy = {}
   tester.jobLog = {}
   for i = 1,25 do
      table.insert(tester.jobLog,{})
   end
   tester.nWorkers = 10 
end

tester.prepareWorker = function(worker)
   tester.jobsSeenBy[worker.index] = {}
end

local x = 0

tester.generateJob = function(worker)
   local i = worker.index
   local jobDescriptor = {MRSIM = {
      testJob = {
         map= function(args) 

            print("MAP",args.testnumber)
            table.insert(tester.jobsSeenBy[i], args.testnumber)
            table.insert(tester.jobLog[args.testnumber], "MAP " .. (((x % 13 > 9) and "FAILURE") or "SUCCESS") )
            --table.insert(tester.jobLog[args.testnumber], "MAP SUCCESS") 
            
            if x % 13 > 9 then
               --error("ERROR!")
               os.exit()
            end
         end,
         reduce = function(args, results) 
            x = x + 1
            table.insert(tester.jobsSeenBy[i], "REDUCE " .. args.testnumber)

            local resultWork = {}
            for j,entry in ipairs(results) do
               table.insert(resultWork, entry:sub(12,18) .. " ")
            end
            table.insert(tester.jobLog[args.testnumber], "REDUCE " .. table.concat(resultWork))
            print("REDUCE", args.testnumber, results)   
--            if x % 7 == 3 then
--               os.exit()
--            end
         end,
      },
      config = {skip = true, nodenum = ((i % 4) + 1)}
   }}

   print("NODE NUM", ((i % 4) + 1))

return jobDescriptor
end

tester.evaluateCode = function(i, client)
--   print("jobs seen by worker " .. i .. ": " .. #tester.jobsSeenBy[i])
--   print(tester.jobsSeenBy[i])

   if i == 1 then

      for j,entry in ipairs(tester.jobLog) do
         print("JOB " .. j, tester.jobLog[j])
      end
      -- check to see that queue variables aren't messed up
      client.zcard("MRQUEUE:0:MRSIM", function(res) print("ZCARD MRQUEUE:0:MRSIM",res,0) end) 
      client.zcard("MRQUEUE:1:MRSIM", function(res) print("ZCARD MRQUEUE:1:MRSIM",res,0) end) 
      client.zcard("MRQUEUE:2:MRSIM", function(res) print("ZCARD MRQUEUE:2:MRSIM",res,0) end) 
      client.zcard("MRQUEUE:3:MRSIM", function(res) print("ZCARD MRQUEUE:3:MRSIM",res,0) end) 
      client.zcard("MRQUEUE:4:MRSIM", function(res) print("ZCARD MRQUEUE:4:MRSIM",res,0) end) 
      client.hlen("MRWAITING:MRSIM", function(res) print("MRWAITING:MRSIM", res, 0) end)
      client.hlen("MRJOBS:MRSIM", function(res) print("MRJOBS:MRSIM", res, 0) end)
      client.hlen("MRPROGRESS:MRSIM", function(res) print("MRPROGRESS:MRSIM", res, 0) end)
      client.hlen("MRRESULTS:MRSIM", function(res) print("MRRESULTS:MRSIM", res, 0) end)
      

   end
end

tester.run()
