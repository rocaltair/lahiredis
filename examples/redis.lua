local function showInfo()
	for i, v in pairs(lahiredis) do
		print(string.format("lahiredis.%s = %s", i, tostring(v)))
	end
end

local function test()
	local conn = false
	conn = lahiredis.new("localhost", 6379)
	if not conn then
		print("new failed!")
		return 
	end
	print("======================")
	print("conn", conn)
	print("======================")
	for i, v in pairs(getmetatable(conn)) do
		print("meta for conn", i, v)
	end
	for i, v in pairs(getmetatable(conn).__index) do
		print("method for conn", i)
	end
	print("======================")

	conn:connect()

	conn:set_disconnect_cb(function()
		print("disconnected!!!")
	end)

	conn:set_connect_cb(function(status)
		print(string.format("is connected=%s", tostring(conn:is_connected())))
		conn:exec(function(status, result, err)
			print("reply in lua>", status, result, err, type(result))
			conn:exec(function(status, result, err)
				print("reply in lua>", status, result, err, type(result))
				local ret = msgpack.unpack(result)
				conn:disconnect()
			end, "GET", "msg")
		end, "SET", "msg", "hiredis")
	end)
end

showInfo()
test()


