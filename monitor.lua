local async = require 'async'
require('pl.text').format_operator()
local rc = require 'redis-async'
local rq = require 'redis-queue'

local c = async.repl.colorize
local fiber = async.fiber
local wait = fiber.wait

local host = arg[1] or 'localhost'
local port = 8080

-- connect to Redis:
local client
local queueClient
rc.connect({host=host, port=6379}, function(c)
   client = c
   rq(client, function(newqueue)
      queueClient = newqueue
      print ("READY!")
   end) 
end)

local function gettimeago(ts)
   local now = os.time()
   local diff = now - ts

   if diff < 60 then
      return diff .. " seconds ago"
   elseif diff < 60 * 60 then
      return  math.floor(diff / 60) .. " minute(s) ago"
   elseif diff < 24 * 60 * 60 then
      return math.floor(diff / (60 * 60)) .. " hour(s) ago"
   else
      return  math.floor(diff / (24 * 60 * 60)) .. " day(s) ago"
   end
end

local function url_encode(str)
  if (str) then
    str = string.gsub (str, "\n", "\r\n")
    str = string.gsub (str, "([^%w %-%_%.%~])",
        function (c) return string.format ("%%%02X", string.byte(c)) end)
    str = string.gsub (str, " ", "+")
  end
  return str	
end

local function url_decode(str)
  str = string.gsub (str, "+", " ")
  str = string.gsub (str, "%%(%x%x)",
      function(h) return string.char(tonumber(h,16)) end)
  str = string.gsub (str, "\r\n", "\n")
  return str
end

--                  <meta http-equiv="refresh" content="5">
local header = [[
               <head>
                  <script src="//ajax.googleapis.com/ajax/libs/jquery/1.10.2/jquery.min.js"></script>
                  <style type="text/css">
                     html,body {font-family: Helvetica;}
                     h1 {color:#22a;}
                     h2 {color:#22e;}
                     table {
                        font-family: verdana,arial,sans-serif;
                        font-size:11px;
                        color:#333333;
                        border-width: 1px;
                        border-color: #666666;
                        border-collapse: collapse;
                     }
                     table th {
                        border-width: 1px;
                        padding: 8px;
                        border-style: solid;
                        border-color: #666666;
                        background-color: #dedede;
                     }
                     table td {
                        border-width: 1px;
                        padding: 8px;
                        border-style: solid;
                        border-color: #666666;
                        background-color: #ffffff;
                     }
                  </style>
               </head>
]]

local mainPage = function(req, res)
   -- collect data:

   local qconfig = queueClient.config.configtbl or {}

   local queues = {}
   local lbqueues = {}

   -- identify keys
   for queue,qType in pairs(qconfig) do
      if qType == "LBQUEUE" then
         table.insert(lbqueues, queue)
      else
         table.insert(queues, queue)
      end
   end

   -- format table:
   local qrows = {}
   local lbqrows = {}


   for i,key in ipairs(queues) do 
      local vals = wait({client.llen, client.hlen}, {{"QUEUE:" .. key},{"UNIQUE:"..key}})
      local row = [[
      <tr> 
      <td> ${name} </td>
      <td> ${queue} </td> 
      <td> ${hashes} </td> 
      <td><a href="${showurl}">Show</a></td> 
      <td><a href="${clearurl}" onclick="return confirm('Are you sure?')">Clear Queue</a></td> 
      </tr>
      ]] % {
         name = key,
         queue = vals[1][1] or 0,
         hashes = vals[2][1] or 0,
         showurl = "/showq?queue="..key,
         clearurl = "/clear?queue="..key,
      }
      table.insert(qrows, row)
   end
   qrows = table.concat(qrows)


   for i,key in ipairs(lbqueues) do 
      local vals = wait({client.zcard, client.hlen, client.hlen, client.scard}, {{"LBQUEUE:" .. key},{"LBJOBS:" .. key},{"LBBUSY:" .. key},{"LBWAITING:" .. key}})

      local row = [[
      <tr> 
      <td> ${name} </td>
      <td> ${queue} </td> 
      <td> ${jobs} </td> 
      <td> ${busy} </td> 
      <td> ${waiting} </td> 
      <td><a href="${showurl}">Show</a></td> 
      <td><a href="${clearurl}" onclick="return confirm('Are you sure?')">Clear Queue</a></td> 
      </tr>
      ]] % {
         name = key,
         queue = vals[1][1] or 0,
         jobs = vals[2][1] or 0,
         busy = vals[3][1] or 0,
         waiting = vals[4][1] or 0,
         showurl = "/showlbq?queue="..key,
         clearurl = "/clear?queue="..key.."&type=LB"
      }
      table.insert(lbqrows, row)
   end
   lbqrows = table.concat(lbqrows)


   local workerinfo = wait({client.hgetall, client.hgetall}, {{"RESERVED:RUNNINGJOBS"},{"RESERVED:RUNNINGTIMES"}})

   local wrows = {}
   
   local runningworkers = {}
   local runningtimes = {}
   local allworkers = {}
   local workerlist = {}

   for i=1,#workerinfo[1][1],2 do
      local workername = workerinfo[1][1][i]
      local jobJson = workerinfo[1][1][i+1]

      local x,y,jobqueue = jobJson:find('("queue":".-")')
      local x,y,jobhash = jobJson:find('("hash":".-")')
      local x,y,jobname = jobJson:find('("name":".-")')

      runningworkers[workername] = jobqueue .. ", " .. jobname .. ", " .. jobhash
      if not allworkers[workername] then
         table.insert(workerlist, workername)
      end
      allworkers[workername] = true
   end

   for i=1,#workerinfo[2][1],2 do
      local workername = workerinfo[2][1][i]
      local jobtime = workerinfo[2][1][i+1]
      runningtimes[workername] = jobtime
      if not allworkers[workername] then
         table.insert(workerlist, workername)
      end
      allworkers[workername] = true
   end

   table.sort(workerlist)

   for _,worker in ipairs(workerlist) do
      local row = [[
      <tr> 
      <td> ${name} </td>
      <td> ${lasttime} </td> 
      <td> ${lastjob} </td> 
      </tr>
      ]] % {
         name = worker,
         lasttime = gettimeago(runningtimes[worker]),
         lastjob = runningworkers[worker] or "Waiting for job",
      }
      table.insert(wrows, row)
   end

   wrows = table.concat(wrows)


   local failures = wait({client.hgetall, client.hgetall, client.hgetall}, 
   {{"RESERVED:FAILEDJOBS"},{"RESERVED:FAILEDERROR"}, {"RESERVED:FAILEDTIME"}})


   local failedJobs = {}
   local failureReasons = {}
   local failureTimes = {}

   for i=1,#failures[1][1],2 do
      local jobHash = failures[1][1][i]
      local jobJson = failures[1][1][i+1]
      failedJobs[jobHash] = jobJson
   end

   for i=1,#failures[2][1],2 do
      local jobHash = failures[2][1][i]
      local jobError = failures[2][1][i+1]
      failureReasons[jobHash] = jobError
   end

   for i=1,#failures[3][1],2 do
      local jobHash = failures[3][1][i]
      local failureTime = failures[3][1][i+1]
      failureTimes[jobHash] = failureTime
   end


   local frows = {}

   for k,v in pairs(failedJobs) do

      local row = [[
      <tr> 
      <td> ${time} </td> 
      <td> ${name} </td> 
      <td> ${err} </td> 
      <td><a href="${showurl}">Show</a></td>
      <td><a href="${retryurl}" onclick="return confirm('Are you sure?')">Retry</a></td>
      <td><a href="${clearurl}" onclick="return confirm('Are you sure?')">Clear</a></td>
      </tr>
      ]] % {
         time = gettimeago(failureTimes[k]),
         name = k,
         err = tostring(stringx.shorten(failureReasons[k], 100, false)),
         retryurl = "/retryjob?id=" .. url_encode(k),
         showurl = "/showjob?id=" .. url_encode(k),
         clearurl = "/clearjob?id=" .. url_encode(k),

      }
      table.insert(frows, row)
   end

   frows = table.concat(frows)


   -- full page:
   local page = [[
   <html>
   ${header}
   <body>
   <h1>Redis Queue</h1>
   <table>
   <tr> 
   <th>Standard Queues</th> 
   <th>Jobs Waiting</th> 
   <th>Unique</th> 
   </tr>
   ${quevals}
   </table>

   <table>
   <tr> <th>Load Balanced Queues</th> 
   <th>Jobs Queued</th> 
   <th>Jobs Known</th> 
   <th>Jobs Busy</th> 
   <th>Jobs Waiting</th> 
   </tr>
   ${lbquevals}
   </table>

   <table>
   <tr> <th>Busy Workers</th><th>Last Seen</th><th>Current Job</th> </tr>
   ${workervals}
   </table>

   <a href="/clearfailed">Clear Failed Jobs </a>

   <table>
   <tr><th>Time</th><th>Failed</th><th>Reason</th><th>Show</th><th>Retry</th><th>Clear</th> </tr>
   ${failedvals}
   </table>

   </body>
   </html>
   ]] % {header = header, quevals = qrows, lbquevals = lbqrows, workervals = wrows, failedvals = frows}

   -- html response:
   res(page, {['Content-Type']='text/html'})

end

local clearfailed = function(res)
   client.del("RESERVED:FAILEDJOBS")
   client.del("RESERVED:FAILEDERROR")
   res("failed jobs cleared", {['Content-Type']='text/html'})
end

local clearQueue = function(req,res)
   local x,y,queue= req.url.query:find("queue=(%a+)")
   local z,w,qtype= req.url.query:find("type=(%a+)")

   if qtype == "LB" then
      client.del("LBQUEUE:"..queue)
      client.del("LBJOBS:"..queue)
      client.del("LBBUSY:"..queue)
      client.del("LBWAITING:"..queue)
   else 
      client.del("QUEUE:"..queue)
      client.del("UNIQUE:"..queue)
   end
   res("OK "..queue.." cleared", {['Content-Type']='text/html'})
end

local showJob = function(req, res)

   local _,_,jobname = req.url.query:find("id=(.*)")

   jobname = url_decode(jobname)

   local vals = wait({client.hget, client.hget}, {{"RESERVED:FAILEDJOBS", jobname},{"RESERVED:FAILEDERROR", jobname}})

   local page = [[
   <html>
   ${header}
   <body>
   <h1>Redis Queue</h1>
   <table>
   <tr> 
   <th>${name}</th> 
   </tr>
   <tr>
   <td>${args}</td>
   </tr>
   <tr>
   <td>${reason}</td>
   </tr>
   </table>
 
   <p>
   <a href="${retryurl}">retry</a>
   </p>
   <p>
   <a href="${clearurl}">clear</a>
   </p>

   </body>
   </html>
   ]] % {header = header, name = jobname, args = vals[1][1], reason = vals[2][1], retryurl = "/retryjob?id=" .. url_encode(jobname), clearurl = "/clearjob?id=" .. url_encode(jobname)}

   -- html response:
   res(page, {['Content-Type']='text/html'})

end

local showQ = function(req, res)
   local _,_,queue= req.url.query:find("queue=(.*)")
   local vals = wait({client.lrange, client.hkeys}, {{"QUEUE:" .. queue, 0, -1},{"UNIQUE:"..queue}})

   local queuedjobs = vals[1][1]
   local uniquejobs = vals[2][1]

   local jobrows = {}

   for _,job in ipairs(queuedjobs) do 
      local row = [[
      <tr> 
      <td> ${job} </td> 
      </tr>
      ]] % {
         job = job,
      }
      table.insert(jobrows, row)
   end

   jobrows = table.concat(jobrows)

      
   local uniquejobrows = {}


   for _,jobhash in ipairs(uniquejobrows) do 
      local row = [[
      <tr> 
      <td> ${jobhash} </td> 
      </tr>
      ]] % {
         jobhash = jobhash
      }

      table.insert(uniquejobrows, row)
   end

   uniquejobrows = table.concat(uniquejobrows)

   
   -- full page:
   local page = [[
   <html>
   ${header}
   <body>
   
   <table>
   <tr> <th>Jobs in the Queue</th></tr>
   <tr> <th>Job</th> </tr>
   ${jobrows}
   </table>

   <table>
   <tr> <th>Unique Jobs</th></tr>
   <tr><th>Identifier</th> </tr>
   ${uniquejobrows}
   </table>

   </body>
   </html>
   ]] % {header = header, jobrows = jobrows, uniquejobrows = uniquejobrows}


   -- html response:
   res(page, {['Content-Type']='text/html'})

end

local showLBQ = function(req, res)
   local _,_,queue= req.url.query:find("queue=(.*)")
   local vals = wait({client.zrange, client.hgetall, client.hgetall, client.smembers}, {{"LBQUEUE:" .. queue, 0, -1, "WITHSCORES"},{"LBJOBS:" .. queue},{"LBBUSY:" .. queue},{"LBWAITING:" .. queue}})

   local queuedJobs = vals[1][1]
   local knownJobs = vals[2][1]
   local busyJobs = vals[3][1]
   local waitingJobs = vals[4][1]

   local jobrows = {}

   for i = 1,#queuedJobs,2 do 
      local row = [[
      <tr> 
      <td> ${score} </td>
      <td> ${job} </td> 
      </tr>
      ]] % {
         score = queuedJobs[i+1],
         job = queuedJobs[i],
      }
      table.insert(jobrows, row)
   end

   jobrows = table.concat(jobrows)

      
   local knownjobrows = {}

   for i = 1,#knownJobs,2 do 
      local row = [[
      <tr> 
      <td> ${hashname} </td>
      <td> ${job} </td> 
      </tr>
      ]] % {
         hashname = knownJobs[i],
         job = knownJobs[i+1],
      }
      table.insert(knownjobrows, row)
   end

   knownjobrows = table.concat(knownjobrows)

   local busyjobrows = {}

   for i = 1,#busyJobs,2 do 
      local row = [[
      <tr> 
      <td> ${workername} </td>
      <td> ${job} </td> 
      </tr>
      ]] % {
         job = busyJobs[i],
      }
      table.insert(busyjobrows, row)
   end

   busyjobrows = table.concat(busyjobrows)


   local waitingjobcounts = {}
   
   for _,job in ipairs(waitingJobs) do
      local x,y,jobhash = job:find('"hash":"(.-)"')
      waitingjobcounts[jobhash] = (waitingjobcounts[jobhash] or 0 ) + 1
   end
   
   local waitingjobrows = {}

   for jobhash,jobcount in pairs(waitingjobcounts) do 
      local row = [[
      <tr> 
      <td> ${jobhash} </td>
      <td> ${jobcount} </td> 
      </tr>
      ]] % {
         jobhash = jobhash,
         jobcount = jobcount,
      }
      table.insert(waitingjobrows, row)
   end

   waitingjobrows = table.concat(waitingjobrows)


   -- full page:
   local page = [[
   <html>
   ${header}
   <body>
   
   <table>
   <tr> <th>Jobs in the Queue</th></tr>
   <tr> <th>Score</th> 
   <th>Job</th> 
   </tr>
   ${jobrows}
   </table>

   <table>
   <tr> <th>Known Jobs</th></tr>
   <tr><th>Identifier</th> 
   <th>Job</th> 
   </tr>
   ${knownjobrows}
   </table>

   <table>
   <tr> <th>Busy Jobs</th></tr>
   <tr><th>Job</th> 
   </tr>
   ${busyjobrows}
   </table>

   <table>
   <tr> <th>Waiting Jobs</th></tr>
   <tr><th>Identifier</th> 
   <th>Count</th> 
   </tr>
   ${waitingjobrows}
   </table>



   </body>
   </html>
   ]] % {header = header, jobrows = jobrows, knownjobrows = knownjobrows, busyjobrows = busyjobrows, waitingjobrows = waitingjobrows}


   -- html response:
   res(page, {['Content-Type']='text/html'})


end

local retryJob = function(req,res)
   local _,_,jobname = req.url.query:find("id=(.*)")

   jobname = url_decode(jobname)

   local args = wait(client.hget, {"RESERVED:FAILEDJOBS", jobname})

   queueClient:reenqueue(jobname, args)

   res("OK ".. jobname .." requeued", {['Content-Type']='text/html'})

end

local clearFailedJob = function(req,res)
   local _,_,jobname = req.url.query:find("id=(.*)")

   jobname = url_decode(jobname)

   local args = wait(client.hget, {"RESERVED:FAILEDJOBS", jobname})

   wait({client.hdel, client.hdel}, {{"RESERVED:FAILEDJOBS", jobname},{"RESERVED:FAILEDERROR", jobname}})

   res("OK ".. jobname .." cleared", {['Content-Type']='text/html'})

end


-- listen up:
async.http.listen('http://0.0.0.0:'..port, function(req,res)
   fiber(function()
      -- log:
      print(c.blue(req.method) .. ' @ ' .. c.red(req.url.path))

      -- resp for /
      if req.url.path == '/' then
         mainPage(req,res)
      elseif req.url.path == "/clearfailed" then
         clearfailed(res)
      elseif req.url.path == "/clear" then
         clearQueue(req,res)
      elseif req.url.path == "/showjob" then
         showJob(req,res)
      elseif req.url.path == "/showq" then
         showQ(req,res)
      elseif req.url.path == "/showlbq" then
         showLBQ(req,res)
      elseif req.url.path == "/retryjob" then
         retryJob(req,res)
      elseif req.url.path == "/clearjob" then
         clearFailedJob(req,res)
      elseif req.url.path == "/die" then
         os.exit()
      end
   end)
end)

-- Start event loop
print('http monitor listening on port: ' .. c.blue(port))
async.go()
