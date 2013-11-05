local async = require 'async'
require('pl.text').format_operator()
local rc = require 'redis-async'

local c = async.repl.colorize
local fiber = async.fiber
local wait = fiber.wait

local host = arg[1] or 'localhost'
local port = 8080

-- connect to Redis:
local client
rc.connect({host=host, port=6379}, function(c)
   client = c
end)

-- listen up:
async.http.listen('http://0.0.0.0:'..port, function(req,res)
   fiber(function()
      -- log:
      print(c.blue(req.method) .. ' @ ' .. c.red(req.url.path))

      -- resp for /
      if req.url.path == '/' then
         -- collect data:
         local resp = wait(client.keys, {'*'})

         local queues = {}
         local lbqueues = {}

         -- identify keys
         for i,key in ipairs(resp) do
            local x,y,name = key:find("LBQUEUE:(.*)") 

            if name then
               table.insert(lbqueues, name)
            else
               local x,y,name = key:find("QUEUE:(.*)")
               if name then
                  table.insert(queues, name)
               end
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
                  <td><a href="${clearurl}" onclick="return confirm('Are you sure?')">Clear Queue</a></td> 
               </tr>
            ]] % {
               name = key,
               queue = vals[1][1],
               hashes = vals[2][1],
               clearurl = "/clear?queue="..key
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
                  <td><a href="${clearurl}" onclick="return confirm('Are you sure?')">Clear Queue</a></td> 
               </tr>
            ]] % {
               name = key,
               queue = vals[1][1],
               jobs = vals[2][1],
               busy = vals[3][1],
               waiting = vals[4][1],
               clearurl = "/clear?queue="..key.."&type=LB"
            }
            table.insert(lbqrows, row)
         end
         lbqrows = table.concat(lbqrows)


         local jobs = wait(client.hgetall, {"RESERVED:RUNNINGJOBS"})

         local wrows = {}

         for i = 1,#jobs,2 do
            local row = [[
            <tr> 
            <td> ${name} </td>
            <td> ${val} </td> 
            </tr>
            ]] % {
               name = jobs[i],
               val = jobs[i+1]
            }
            table.insert(wrows, row)
         end

         wrows = table.concat(wrows)

         local failed = wait(client.lrange, {"RESERVED:FAILED",0,-1})

         local frows = {}

         for i = 1,#failed,2 do
            local row = [[
            <tr> 
            <td> ${job} </td> 
            <td> ${retry} </td> 
            </tr>
            ]] % {
               job = failed[i],
               retry = "/retry"
            }
            table.insert(frows, row)
         end

         frows = table.concat(frows)

       
         -- full page:
         local page = [[
            <html>
               <head>
                  <meta http-equiv="refresh" content="5">
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
                     <tr> <th>Busy Workers</th> <th>Current Job</th> </tr>
                     ${workervals}
                  </table>


                  
                  <table>
                     <tr> <th>Failed Jobs</th> <th>Retry</th> </tr>
                     ${failedvals}
                  </table>

               </body>
            </html>
         ]] % {quevals = qrows, lbquevals = lbqrows, workervals = wrows, failedvals = frows}

         -- html response:
         res(page, {['Content-Type']='text/html'})
      elseif req.url.path:find("/clear")then
         print(pretty.write(req.url))
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
   end)
end)

-- Start event loop
print('http monitor listening on port: ' .. c.blue(port))
async.go()
