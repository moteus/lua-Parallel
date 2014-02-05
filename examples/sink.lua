local Parallel = require "Parallel"

local T = string.dump

Parallel.For(1, 100, T(function(thread_no)
  FOR(function(i) return thread_no, math.pow(i, 2), math.sqrt(i) end)
end), print)
