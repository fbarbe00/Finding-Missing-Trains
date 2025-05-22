api_version = 4

local Set = require('lib/set')
local Sequence = require('lib/sequence')
local Tags = require('lib/tags')

function setup()
	return {
		properties = {
			-- weight_name                    = 'duration',
			weight_name                    = 'routability',
			left_hand_driving              = false,

			continue_straight_at_waypoint  = false,
			max_speed_for_map_matching     = 220/3.6, -- speed conversion to m/s
			
			u_turn_penalty                 = 60 * 6,
			use_turn_restrictions          = true,
			turn_duration                  = 20,
			max_angle                      = 30,

			-- secondary_speed                = 30,
			-- speed                          = 160,
		},

		excludable = {
			Set {"highspeed"},
			Set {"notelectrified"},
			Set {"notelectrified", "highspeed"},
		},

		classes = {
			"highspeed",
			"electrified",
			"notelectrified",
			"station",
			"platform",
			"1435",
			"1668",
		},

		restrictions = Sequence {
		  'train' -- see which ones are valid
		},

		default_mode              = mode.driving,
		default_speed             = 160,

		relation_types = Sequence {
			"route",
			"public_transport",
		},

		barriers = Set {
			"buffer_stop",
			-- "derail"
		},

		secondary = Set {
			"siding",
			"spur",
			"yard",
			"industrial",
		},

		exclude_usages = Set {
			"military",
			"tourism",
			"test",
			"scientific",
			"industrial",
		},

		exclude_services = Set {
			-- "yard",
			-- "siding"
		},

		allowed_types = Set {
			"rail",
			-- "turntable", -- too slow
			-- "ferry", -- no
			-- "traverser"  -- no
		},

		highspeed_networks = Set {
			"TGV",
			"DB InterCityExpress",
			"AVE",
			"TGV InOui",
			"TGV Lyria"
		},

		regional_networks = Set {
			"RER",
			"U-Bahn",
			"S-Bahn",
		},

		bidirectional_highspeed = Set {
			428683861,
		},

		speeds = {
			default = 160,
			main = 160,
			secondary = 30,
			highspeed = 250, -- minimum
		},

		vehicle_speeds = {
			ice3 = 320,
		},

		max_vehicle_speed = 320, -- hm/h
		vehicle_accel = 1, -- m/s^2
		max_global_speed = 320,
		max_speed_bidirectionality = 120, -- km/h

		preferred_direction = {
			[250774955] = "backward",
			[447930149] = "backward",
			[240240159] = "forward",
			[184032018] = "forward",
			[453436353] = "forward",
			[139764089] = "forward",
			[1043811791] = "backward",
			[510723674] = "forward",
			[32126661] = "forward",
			[984405362] = "backward", -- Karlsruhe -- Baden-Baden
			[275162998] = "forward", -- Bonn -- Köln
			[427342411] = "forward", -- Koblenz -- Bonn
			[248693410] = "backward", -- Bonn -- Köln 
			[38484610] = "backward", -- Bonn -- Köln
			[150006369] = "backward", -- Köln
			[173628672] = "backward", -- Magdeburg -- Braunschweig

			[68423760] = "backward", -- Donauwörth -- Augsburg
			[72548960] = "backward", -- Donauwörth -- Augsburg
			[72548922] = "backward", -- Donauwörth -- Augsburg
			[44969605] = "backward", -- Donauwörth -- Augsburg
			[44969606] = "backward", -- Donauwörth -- Augsburg
			[44966480] = "backward", -- Donauwörth -- Augsburg
			[44966481] = "backward", -- Donauwörth -- Augsburg
			[44966476] = "backward", -- Donauwörth -- Augsburg
			[44966477] = "backward", -- Donauwörth -- Augsburg
			[44966471] = "backward", -- Donauwörth -- Augsburg
			[44966472] = "backward", -- Donauwörth -- Augsburg
			[44964568] = "backward", -- Donauwörth -- Augsburg
			[45030321] = "backward", -- Donauwörth -- Augsburg
			[68423760] = "backward", -- Donauwörth -- Augsburg
			[68423760] = "backward", -- Donauwörth -- Augsburg
			[68423760] = "backward", -- Donauwörth -- Augsburg
			[68423760] = "backward", -- Donauwörth -- Augsburg
			[248186770] = "backward", -- Osnabruck -- Bremen

			[23894694] = "forward", -- Bremen-Oldenburg

			-- [984405362] = "backward",
			-- [984405362] = "backward",
			-- [984405362] = "backward",
			-- [984405362] = "backward",
			-- [984405362] = "forward",
			-- [984405362] = "forward",
			-- [984405362] = "forward",
			-- [984405362] = "forward",
			-- [984405362] = "forward",
			-- [984405362] = "forward",
			-- [984405362] = "forward",
			-- [984405362] = "forward",
		},

		allow_bidirectionality = Set{
			222822589, -- near lyon
			292443375, -- north east of France
			615638164,
			615638166,
			684678896,

			256602703,
			303247915,
			303247926,
			303247931,
			303247934,
			303247936,
			303247937, -- solving a routing problem in Lille
			36334755,
			36334756,
			689166204,


			129266984, -- correction at Besançon Franche-Comté TGV
			138100602,
			494748648,
			494748649,
			615981804,
			615981815,
			615981870,
			615981879,

			395173663, -- Mâcon - Loché TGV and Aix-les-Bains le Revard & Paris Gare de Lyon Hall 1 - 2 and Aix-les-Bains le Revard & Paris Gare de Lyon Hall 1 - 2 and Bellegarde

			445002120, -- Toulon and Hyères

			247072697, -- Welkenraedt, Place de la Gare, Welkenraedt, Verviers, Liège, Wallonia, 4840, Belgium -- Bahnhof Eupen, Bahnhofstraße, Nispert, Eupen (Altgemeinde), Eupen, Verviers, Liège, Wallonia, 4700, Belgium
			684678896,
			
			20452553, -- Frankfurt(M) Flughafen Fernbf -> Mainz Hbf
			147949588, -- Frankfurt(M) Flughafen Fernbf -> Köln Hbf

			114126878, -- Köln -- Bonn
			114126887, -- Köln -- Bonn
			
			437700054, -- Frankfurt Süd -- Würzburg
			286339642, -- Frankfurt Süd -- Würzburg
			
			98538622, -- Magdeburg
			98538626, -- Magdeburg
			
			69281752, -- Hildesheim
			887639391, -- Göttingen
			42488151, -- Wolfsburg

			51204926, -- Schnellfahrstrecke Köln-Rhein/Main
			51204927, -- Schnellfahrstrecke Köln-Rhein/Main

			1008949483, -- Friedrichshafen Stadt
			1008949484, -- Friedrichshafen Stadt
			1008949485, -- Friedrichshafen Stadt
			1008949486, -- Friedrichshafen Stadt
			1009158373, -- Friedrichshafen Stadt
			115653746, -- Friedrichshafen Stadt
			115653752, -- Friedrichshafen Stadt
			198217379, -- Friedrichshafen Stadt
			23694501, -- Friedrichshafen Stadt
			437103100, -- Friedrichshafen Stadt
			539944962, -- Friedrichshafen Stadt
			539944963, -- Friedrichshafen Stadt
			791305785, -- Friedrichshafen Stadt
			855473190, -- Friedrichshafen Stadt

			136486108, -- Bayerische Allgäubahn
			136486484, -- Bayerische Allgäubahn
			136486484, -- Bayerische Allgäubahn
			138151537, -- Bayerische Allgäubahn
			210293423, -- Bayerische Allgäubahn
			415991574, -- Bayerische Allgäubahn
			45863176, -- Bayerische Allgäubahn
			45863181, -- Bayerische Allgäubahn
			45866730, -- Bayerische Allgäubahn

			102028787, -- Rosenheim
			415991559, -- Rosenheim
			415991560, -- Rosenheim

			141055391, -- Appenweierer Kurve

			150832680, -- Dresden-Neustadt -> Riesa
			150832686, -- Dresden-Neustadt -> Riesa
			150832705, -- Dresden-Neustadt -> Riesa
			150832712, -- Dresden-Neustadt -> Riesa
			150832723, -- Dresden-Neustadt -> Riesa
			198440116, -- Dresden-Neustadt -> Riesa
			32547556, -- Dresden-Neustadt -> Riesa
			32547559, -- Dresden-Neustadt -> Riesa
			360723894, -- Dresden-Neustadt -> Riesa
			360725700, -- Dresden-Neustadt -> Riesa
			361814640, -- Dresden-Neustadt -> Riesa
			362165877, -- Dresden-Neustadt -> Riesa
			51724437, -- Dresden-Neustadt -> Riesa
			51724445, -- Dresden-Neustadt -> Riesa
			988063127, -- Dresden-Neustadt -> Riesa

			4305692, -- Bremen Neustadt
			4926919, -- Bremen Neustadt
			68567725, -- Bremen Neustadt
			68567728, -- Bremen Neustadt
			864484353, -- Bremen Neustadt

			116525701, -- Augsburg
			141765557, -- Augsburg
			156426693, -- Augsburg
			183503881, -- Augsburg
			183503886, -- Augsburg
			183503888, -- Augsburg
			32431219, -- Augsburg
			32431219, -- Augsburg
			415991581, -- Augsburg
			415991583, -- Augsburg
			415991589, -- Augsburg
			415991589, -- Augsburg
			415991622, -- Augsburg
			415991629, -- Augsburg
			415991644, -- Augsburg
			415991664, -- Augsburg
			436318238, -- Augsburg
			45343000, -- Augsburg
			45343001, -- Augsburg
			45399256, -- Augsburg
			45400637, -- Augsburg
			45403356, -- Augsburg
			67192577, -- Augsburg
			68348047, -- Augsburg
			68348048, -- Augsburg
			68419043, -- Augsburg
			68419045, -- Augsburg
			68622846, -- Augsburg
			68631656, -- Augsburg
			804611575, -- Augsburg
			808165102, -- Augsburg

			400574066, -- Schwarzwaldbahn
			39934122, -- Schwarzwaldbahn
			795729806, -- Schwarzwaldbahn
			821107840, -- Schwarzwaldbahn
			74794970, -- Schwarzwaldbahn
			975672209, -- Schwarzwaldbahn

			334381950, -- Mannheim -- Stuttgart

			132086031, -- Aachen Hbf -> Liège-Guillemins
			132086033, -- Aachen Hbf -> Liège-Guillemins
			132086035, -- Aachen Hbf -> Liège-Guillemins
			132086040, -- Aachen Hbf -> Liège-Guillemins
			132087055, -- Aachen Hbf -> Liège-Guillemins
			132087064, -- Aachen Hbf -> Liège-Guillemins
			13808627, -- Aachen Hbf -> Liège-Guillemins
			27100741, -- Aachen Hbf -> Liège-Guillemins
			27100742, -- Aachen Hbf -> Liège-Guillemins
			326974958, -- Aachen Hbf -> Liège-Guillemins
			326974959, -- Aachen Hbf -> Liège-Guillemins
			742731259, -- Aachen Hbf -> Liège-Guillemins
			742731260, -- Aachen Hbf -> Liège-Guillemins

			114099223, -- Linke Rheinstrecke
			114099226, -- Linke Rheinstrecke
			114099232, -- Linke Rheinstrecke
			29345521, -- Linke Rheinstrecke
			391627746, -- Linke Rheinstrecke

			245682289, -- Schnellfahrstrecke Köln-Rhein/Main  bei FFlugh
			4742754, -- Schnellfahrstrecke Köln-Rhein/Main  bei FFlugh

			136977666, -- Rosenheim

			871746691, -- LGV Nord

			46989585, -- Schwarzwaldbahn

			-- 871746691, -- LGV Nord
		},

		keep = Set {
			222822589,
			1064624784, -- Köln Messe/Deutz
			149622114, -- Köln
			481126042, -- Köln
			149993888, -- Köln
			149993857, -- Köln
			149993863, -- Köln
			149993867, -- Köln
			149993873, -- Köln
			149993877, -- Köln
			149993884, -- Köln
			149993896, -- Köln
			62182022, -- Hamburg Hbf

			-- 149622114, -- Köln
			-- 149622114, -- Köln
			-- 149622114, -- Köln
		},
	}
end


function T ( cond , t , f )
	if cond then return t else return f end
end


function process_node(profile, node, result, relations)

	local railway = node:get_value_by_key("railway")

	if profile.barriers[railway] then
		result.barrier = true
	end

	result.traffic_lights = false
end

function find_value_in_set(value, set)
	if set[value] == true then
		return true
	elseif type(value) == "string" then
		for k,_ in pairs(set) do
			if value:find(k) then
				return true
			end
 		end
	end
	return false
end

function process_way(profile, way, result, relations)
	local data = {
		railway = way:get_value_by_key("railway"),
	}

	if next(data) == nil then
	  return
	end

	if not data.railway then
		return
	end

	data.electrified = T(way:get_value_by_key("electrified") == "no", nil, way:get_value_by_key("electrified"))
	data.voltage = way:get_value_by_key("voltage")
	data.frequency = way:get_value_by_key("frequency")

	local is_electrified = 
		data.electrified ~= nil
		or data.voltage ~= nil
		or data.frequency ~= nil

	if profile.keep[way:id()] then
	-- if not profile.keep[way:id()] then
		print("Keeping", way:id())
	else
		if not profile.allowed_types[data.railway] then
			return
		end

		data.usage = way:get_value_by_key("usage")

		if profile.exclude_usages[data.usage] then
			return
		end

		data.service = way:get_value_by_key("service")

		if profile.exclude_services[data.service] then
			return
		end

		data.gauge = way:get_value_by_key("gauge")

		if not (data.gauge == nil
			or tonumber(data.gauge) == 1435 or string.find(data.gauge, "1435")
			or tonumber(data.gauge) == 1668 or string.find(data.gauge, "1668")
			) then
			return
		end

		-- if not is_electrified then
		-- 	return
		-- end

		data.importance = way:get_value_by_key("importance") -- should be national, if regional, then can check if TGV circulate on it

		if data.importance == "regional" then
			local rel_id_list = relations:get_relations(way)
			local is_regional = nil
			local is_highspeed = nil
			for i, rel_id in ipairs(rel_id_list) do
				local parent_rel = relations:relation(rel_id)
				if parent_rel:get_value_by_key('type') == 'route' and parent_rel:get_value_by_key('route') == 'train' then
					is_highspeed = parent_rel:get_value_by_key('highspeed') == 'yes'
						or parent_rel:get_value_by_key('service') == 'high_speed'
						or find_value_in_set(parent_rel:get_value_by_key('network'), profile.highspeed_networks)
						or find_value_in_set(parent_rel:get_value_by_key('name'), profile.highspeed_networks)

					if is_highspeed then
						-- print(parent_rel:get_value_by_key('name'), "is highspeed")
						break -- break automatically
					end

					is_regional = is_regional
						or parent_rel:get_value_by_key('service') == 'regional'
						or parent_rel:get_value_by_key('passenger') == 'suburban'
						or find_value_in_set(parent_rel:get_value_by_key('network'), profile.regional_networks)
						or find_value_in_set(parent_rel:get_value_by_key('name'), profile.regional_networks)

					if is_regional then
						-- print(parent_rel:get_value_by_key('name'), "is regional")
					end
				end
			end
			if is_regional and not is_highspeed then
				-- regional only
				-- print(data.name, "is regional")
				return
			end
		end
	end


	
	data.name = way:get_value_by_key("name")
	data.ref = way:get_value_by_key("ref")
	data.highspeed = way:get_value_by_key("highspeed")
	data.traffic_mode = way:get_value_by_key("railway:traffic_mode")
	data.preferred_direction = way:get_value_by_key("railway:preferred_direction")
	data.bidirectional = way:get_value_by_key("railway:bidirectional")
	data.track_ref = way:get_value_by_key("railway:track_ref")

	local is_highspeed = data.highspeed == "yes"
	local is_main = data.usage == "main"

	-- Determine Speed
	local fw_speed, bw_speed = Tags.get_forward_backward_by_key(way,data,'maxspeed')
	-- print("Speeds", type(fw_speed), type(bw_speed))

	if type(fw_speed) == "string" and fw_speed:find("mph") then
		-- print(fw_speed)
		fw_speed, _ = fw_speed:gsub(" mph", "")
		fw_speed, _ = fw_speed:gsub("mph", "")
		fw_speed = tonumber(fw_speed)
		if fw_speed ~= nil then
			fw_speed = T(fw_speed, fw_speed * 1.609344, nil)
		end
	else
		fw_speed = tonumber(fw_speed)
	end

	if type(bw_speed) == "string" and bw_speed:find("mph") then
		bw_speed, _ = bw_speed:gsub(" mph", "")
		bw_speed, _ = bw_speed:gsub("mph", "")
		bw_speed = tonumber(bw_speed)
		if bw_speed ~= nil then
			bw_speed = T(bw_speed, bw_speed * 1.609344, nil)
		end
	else
		bw_speed = tonumber(bw_speed)
	end

	if fw_speed == nil or bw_speed == nil then
		local is_secondary = profile.secondary[data.service] == true or profile.secondary[data.usage] == true

		if is_secondary then
			fw_speed = tonumber(T(fw_speed, fw_speed, profile.speeds.secondary))
			bw_speed = tonumber(T(bw_speed, bw_speed, profile.speeds.secondary))
		elseif is_highspeed then
			fw_speed = tonumber(T(fw_speed, fw_speed, profile.speeds.highspeed))
			bw_speed = tonumber(T(bw_speed, bw_speed, profile.speeds.highspeed))
		elseif is_main then
			fw_speed = tonumber(T(fw_speed, fw_speed, profile.speeds.main))
			bw_speed = tonumber(T(bw_speed, bw_speed, profile.speeds.main))
		else
			fw_speed = tonumber(T(fw_speed, fw_speed, profile.speeds.default))
			bw_speed = tonumber(T(bw_speed, bw_speed, profile.speeds.default))
		end
	else
		fw_speed = tonumber(fw_speed)
		bw_speed = tonumber(bw_speed)
	end

	-- if is_main then
	-- 	print("fw, bw", fw_speed, bw_speed)
	-- end

	result.forward_speed = fw_speed -- speed on this way in km/h. Mandatory.
	result.backward_speed = bw_speed

	result.forward_mode = profile.default_mode --Enum 	Mode of travel (e.g. car, ferry). Mandatory. Defined in include/extractor/travel_mode.hpp.
	result.backward_mode = profile.default_mode

	if data.gauge ~= nil and (tonumber(data.gauge) == 1435 or string.find(data.gauge, "1435")) then
		result.forward_classes["1435"] = true
		result.backward_classes["1435"] = true
	end

	if data.gauge ~= nil and (tonumber(data.gauge) == 1668 or string.find(data.gauge, "1668")) then
		result.forward_classes["1668"] = true
		result.backward_classes["1668"] = true
	end

	if is_highspeed or math.min(fw_speed, bw_speed) >= 250 then
		result.forward_rate = fw_speed/3.6	--Float 	Routing weight, expressed as meters/weight (e.g. for a fastest-route weighting, you would want this to be meters/second, so set it to forward_speed/3.6)
		result.backward_rate = bw_speed/3.6	--Float 	""

		result.forward_classes["highspeed"] = true	--Table 	Mark this way as being of a specific class, e.g. result.classes["toll"] = true. This will be exposed in the API as classes on each RouteStep.
		result.backward_classes["highspeed"] = true	--Table 	""

	else
		result.forward_rate = fw_speed/(3.6*2)
		result.backward_rate = bw_speed/(3.6*2)
	end

	if not is_electrified then
		result.forward_classes["notelectrified"] = true	--Table 	Mark this way as being of a specific class, e.g. result.classes["toll"] = true. This will be exposed in the API as classes on each RouteStep.
		result.backward_classes["notelectrified"] = true	--Table 	""
	end

	data.track_ref = way:get_value_by_key("railway:track_ref")

	if data.track_ref ~= nil then
		result.forward_classes["platform"] = true	--Table 	Mark this way as being of a specific class, e.g. result.classes["toll"] = true. This will be exposed in the API as classes on each RouteStep.
		result.backward_classes["platform"] = true	--Table 	""
	end

	-- name: name (ref:ref) (wayid)
	local name = tostring(T(data.name ~= nil, data.name, "N/A"))
	local ref = tostring(T(data.ref, data.ref, "N/A"))
	local id = tostring(way:id())


	if data.track_ref ~= nil then
		-- print(data.track_ref)
		-- error("Stop!")
	end

	result.name = name.." ("..ref..")"
	-- result.ref = ref
	result.ref = id

	if data.preferred_direction and (not profile.allow_bidirectionality[way:id()]) and (fw_speed >= profile.max_speed_bidirectionality or bw_speed >= profile.max_speed_bidirectionality) then
		local correction = profile.preferred_direction[way:id()]
		if correction then
			data.preferred_direction = correction
			print("Setting direction to ", correction, " for ", "https://www.openstreetmap.org/way/"..way:id())
		end
		-- print("Setting directions")
		local wrong_direction_multiplier = 0

		if data.bidirectional == "regular" then
			if is_highspeed then
				if profile.bidirectional_highspeed[way:id()] then
					wrong_direction_multiplier = 0.5
				else
					wrong_direction_multiplier = 0
				end
				-- wrong_direction_multiplier = 0.25
			else
				wrong_direction_multiplier = 1
			end
		elseif data.bidirectional == "signals" then
			wrong_direction_multiplier = 0.5
		elseif data.bidirectional == "possible" then
			wrong_direction_multiplier = 0
		end

		if data.preferred_direction == "forward" then
			result.backward_rate = result.backward_rate * wrong_direction_multiplier
			if wrong_direction_multiplier <= 0.5 then
				result.backward_restricted = true
			end
			if wrong_direction_multiplier == 0 then
				result.backward_mode = mode.inaccessible
			end
			result.name = T(result.name, result.name.." (FW)", "(FW)")
		elseif data.preferred_direction == "backward" then
			result.forward_rate = result.forward_rate * wrong_direction_multiplier
			if wrong_direction_multiplier <= 0.5 then
				result.forward_restricted = true
			end
			if wrong_direction_multiplier == 0 then
				result.forward_mode = mode.inaccessible
			end
			result.name = T(result.name, result.name.." (BW)", "(BW)")
		else
			result.name = T(result.name, result.name.." (Both)", "(Both)")
		end
	end

	if result.forward_speed > profile.max_global_speed then
		result.forward_speed = profile.max_global_speed
	end
	if result.backward_speed > profile.max_global_speed then
		result.backward_speed = profile.max_global_speed
	end
end

local function r(f)
	return math.floor(f+0.5)
end

function process_turn(profile, turn)
	--[[
		add increased weight when changing lines
	--]]
	turn.weight = 0
	turn.duration = 0

	if r(turn.source_speed/3.6) ~= r(turn.target_speed/3.6) then
		-- print(turn.source_speed, turn.target_speed)
		local v0 = T(turn.source_speed < turn.target_speed, turn.source_speed, turn.target_speed)/3.6
		local v1 = T(turn.source_speed > turn.target_speed, turn.source_speed, turn.target_speed)/3.6
		local a = 3 -- m/s^2

		-- print(r(v0), "m/s", r(v1), "m/s")

		local t1 = (v1 - v0)/a
		local t0 = (v0*t1 + (a*t1^2)/2)/v1

		-- print()

		-- turn.duration = (t1 - t0)*10
		turn.weight = (t1 - t0)*10

		-- print("From "..tostring(r(v0)).." m/s to "..tostring(r(v1)).." m/s adds "..tostring(turn.duration/10).." s.")
		-- print("Now", t1, "Before", t0)
		-- print("dV:", r(v1-v0)*3.6, "km/h dT:", r(t1-t0), "s v0:", v0*3.6, "km/h")
	end


	if math.abs(turn.angle) > profile.properties.max_angle then
		turn.weight = constants.max_turn_weight
		if turn.is_u_turn then
			turn.duration = profile.properties.u_turn_penalty
		else
			turn.duration = constants.max_turn_weight/16 -- there is an overflow otherwise
			-- turn.duration = 3000
		end
	end
end

return {
	setup = setup,
	process_way = process_way,
	process_node = process_node,
	process_turn = process_turn
}

