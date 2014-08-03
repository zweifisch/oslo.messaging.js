debug = require 'debug'

module.exports = (ns)->
	debug: debug "messaging:#{ns}:debug"
	info: debug "messaging:#{ns}:info"
	warn: debug "messaging:#{ns}:warn"
	error: debug "messaging:#{ns}:error"
