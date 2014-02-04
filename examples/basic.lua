local Parallel = require "Parallel"

Parallel.For(1, 100, [[TASK(function(i)
  print(i)
end)]])
