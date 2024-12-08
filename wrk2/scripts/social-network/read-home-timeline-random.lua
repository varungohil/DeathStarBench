local socket = require("socket")
local time = socket.gettime()*1000
math.randomseed(time)
math.random(); math.random(); math.random()

-- load env vars
local max_user_index = tonumber(os.getenv("max_user_index")) or 961

request = function()
  local user_id = tostring(math.random(0, max_user_index - 1))
  local start = tostring(math.random(0, 30))
  local stop = tostring(start + math.random(1, 20))
  local milliseconds_int = math.floor(socket.gettime()*1000)
  local args = "user_id=" .. user_id .. "&start=" .. start .. "&stop=" .. stop .. "&req_id=" .. milliseconds_int
  local method = "GET"
  local path = "/wrk2-api/home-timeline/read?" .. args 

  local headers = {}
  headers["Content-Type"] = "application/x-www-form-urlencoded"
  local response =  wrk.format(method, path, headers, nil)
  return response
end
