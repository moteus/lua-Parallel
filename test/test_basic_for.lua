local Parallel = require "Parallel"

local T = string.dump
local N = 10
local C = 1000

local threads = {}
Parallel.For(1, C, T(function(thread_no)
  local ztimer = require "lzmq.timer"
  FOR(function(i)
    ztimer.sleep(10)
    return thread_no
  end)
end), function(no)
  threads[no] = (threads[no] or 0) + 1
end, N)

assert(N == #threads)

local total = 0
for i = 1, N do total = total + threads[i] end
assert(total == C)

print("Done!")
