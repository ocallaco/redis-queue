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

         -- format table:
         local rows = {}
         for i,key in ipairs(resp) do 
            local val = wait(client.get, {key})
            local row = [[
               <tr> 
                  <td> ${name} </td>
                  <td> ${val} </td> 
               </tr>
            ]] % {
               name = key,
               val = val
            }
            table.insert(rows, row)
         end
         rows = table.concat(rows)
       
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
                     <tr> <th>Keys</th> <th>Vals</th> </tr>
                     ${keyvals}
                  </table>
               </body>
            </html>
         ]] % {keyvals = rows}

         -- html response:
         res(page, {['Content-Type']='text/html'})
      end
   end)
end)

-- Start event loop
print('http monitor listening on port: ' .. c.blue(port))
async.go()