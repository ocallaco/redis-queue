local async = require 'async'

response ="*16\r\n$10\r\nSIMILARITY\r\n$7\r\nLBQUEUE\r\n$4\r\nWALL\r\n$5\r\nQUEUE\r\n$4\r\nSYNC\r\n$7\r\nLBQUEUE\r\n$4\r\nUSER\r\n$7\r\nLBQUEUE\r\n$9\r\nINSTAGRAM\r\n$8\r\nDELQUEUE\r\n$3\r\nNLP\r\n$7\r\nLBQUEUE\r\n$5\r\nMRSIM\r\n$7\r\nMRQUEUE\r\n$5\r\nIMAGE\r\n$7\r\nLBQUEUE\r\n"

local server = async.tcp.listen({host='localhost', port=6379}, function(client)
   print('new connection:',client)
   client.ondata(function(data)
      print('received:',data)
      if response then
         client.write(response)
         response = nil
      end
   end)
   client.onend(function()
      print('client ended')
   end)
   client.onclose(function()
      print('closed.')
      collectgarbage()
      print(collectgarbage("count") * 1024)
   end)
end)

async.repl()

async.go()
