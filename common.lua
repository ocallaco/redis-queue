local json = require 'cjson'


local common = {
   -- reserved 
   RUNNING = "RESERVED:RUNNINGJOBS", -- hash
   RUNNINGSINCE = "RESERVED:RUNNINGTIMES", -- hash
   FAILED = "RESERVED:FAILEDJOBS", -- hash 
   FAILED_ERROR = "RESERVED:FAILEDERROR", -- hash 
   FAILEDTIME = "RESERVED:FAILEDTIME", -- sorted set
   CLEANUP = "RESERVED:CLEANUP:", -- list of jobs failed per queue, awaiting cleanup
}

return common

