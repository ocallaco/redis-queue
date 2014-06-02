Running the test
====

After setting up the queue configuration, run `th test/test_lbwork.lua` in one terminal, and `th test/test_lbenq.lua` in another.

`test_lbwork.lua` should output along the lines of:
```
CONNECTED
[A bunch more...]
Worker1 is up
[...]
test start 1
[...]
{
  b = "test",
  a = 1,
  testnumber = 10
}
[...]
jobs seen by worker 1: 3
[...]
Worker1 closing
[...]
```
