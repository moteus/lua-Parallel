local Parallel = require "Parallel"

local T = string.dump

Parallel.For(1, 100, T(function(thread_no)
  FOR(function(i) print(thread_no .. " :" .. i) end)
end))
