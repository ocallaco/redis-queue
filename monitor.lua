local async = require 'async'
require('pl.text').format_operator()
local c = async.repl.colorize

local port = 8080

async.http.listen('http://0.0.0.0:'..port, function(req,res)
   print('request:',req)

   local rows = {}
   for i = 1,10 do
      local row = [[
         <tr> <td><b>${name}</b></td> <td> ${size} </td> </tr>
      ]] % {
         name = 'queue # '..i,
         size = math.random()
      }
      table.insert(rows, row)
   end
   rows = table.concat(rows)

   local body = [[
      <h1>Redis Queue</h1>
      <table>
      ${rows}
      </table>
   ]] % {rows = rows}
  
   local page = [[
      <html>
      <head>
      <style type="text/css">
         html,body {
            font-family: Helvetica;
         }
         h1 {color:red;}
         p {color:blue;}
      </style>
      </head>
      <body>
      ${body}
      </body>
      </html>
   ]] % {body = body}

   res(page, {['Content-Type']='text/html'})
end)

print('http monitor listening on port: ' .. c.blue(port))

async.go()
