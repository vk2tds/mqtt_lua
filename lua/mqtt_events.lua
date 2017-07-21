-- mqtt_events.lua
--
-- Copyright 2017 Darryl Smith (darryl@radio-active.net.au)
-- License: GNU AFFERO GENERAL PUBLIC LICENSE Version 3 (AGPLv3)
--   based in part on mqtt_library.lua
--   Copyright (c) 2011-2012 by Geekscape Pty. Ltd.
--

require("socket")

local MQTT = {}

MQTT.Utility = require("utility")

MQTT.VERSION = 0x04

MQTT.ERROR_TERMINATE = false      -- Message handler errors terminate process ?

MQTT.DEFAULT_BROKER_HOSTNAME = "localhost"

MQTT.client = {}
MQTT.client.__index = MQTT.client

MQTT.client.DEFAULT_PORT               = 1883
MQTT.client.DEFAULT_KEEP_ALIVE_TIME    = 60 -- seconds (maximum is 65535)
MQTT.client.MAX_PAYLOAD_LENGTH         = 268435455 -- bytes

-- MQTT 3.1 Specification: Section 2.1: Fixed header, Message type

MQTT.message = {}
MQTT.message.TYPE_RESERVED    = 0x00
MQTT.message.TYPE_CONNECT     = 0x01
MQTT.message.TYPE_CONACK      = 0x02
MQTT.message.TYPE_PUBLISH     = 0x03
MQTT.message.TYPE_PUBACK      = 0x04
MQTT.message.TYPE_PUBREC      = 0x05
MQTT.message.TYPE_PUBREL      = 0x06
MQTT.message.TYPE_PUBCOMP     = 0x07
MQTT.message.TYPE_SUBSCRIBE   = 0x08
MQTT.message.TYPE_SUBACK      = 0x09
MQTT.message.TYPE_UNSUBSCRIBE = 0x0a
MQTT.message.TYPE_UNSUBACK    = 0x0b
MQTT.message.TYPE_PINGREQ     = 0x0c
MQTT.message.TYPE_PINGRESP    = 0x0d
MQTT.message.TYPE_DISCONNECT  = 0x0e
MQTT.message.TYPE_RESERVED    = 0x0f

-- MQTT 3.1 Specification: Section 3.2: CONACK acknowledge connection errors
-- http://mqtt.org/wiki/doku.php/extended_connack_codes

MQTT.CONACK = {}
MQTT.CONACK.error_message = {          -- CONACK return code used as the index
  "Unacceptable protocol version",
  "Identifer rejected",
  "Server unavailable",
  "Bad user name or password",
  "Not authorized"
--"Invalid will topic"                 -- Proposed
}


function MQTT.client.create(                                      -- Public API
  client_id,    -- string:   Client Identifier
  keep_alive    -- integer:  Keep Alive Timer
  hostname,     -- string:   Host name or address of the MQTT broker
  port,         -- integer:  Port number of the MQTT broker (default: 1883)
  cb_onconnect, -- integer:  Callback on Connect 
  cb_noconnect) -- integer:  Callback on Connect Failed
                -- return:   mqtt_client table

  local mqtt_client = {}

  setmetatable(mqtt_client, MQTT.client)

      
  mqtt_client.client_id  = client_id
  mqtt_client.keep_alive = keep_alive or MQTT.client.DEFAULT_KEEP_ALIVE_TIME
  mqtt_client.hostname   = hostname
  mqtt_client.port       = port or MQTT.client.DEFAULT_PORT


  mqtt_client.cb_onconnect = cb_onconnect
  mqtt_client.cb_noconnect = cb_noconnect    



  mqtt_client.lwt_topic = nil
  mqtt_client.lwt_message = nil
  mqtt_client.lwt_qos = 0
  mqtt_client.lwt_retain = 0  
  
  -- If Username and Password are blank then use another form of auth...
  mqtt_client.username = nil
  mqtt_client.password = nil
  mqtt_client.secure = false




  mqtt_client.net = nil

  mqtt_client.connected     = false


  -- Not sure about the rest


  mqtt_client.cb_connect = nil  
  mqtt_client.cb_onoffline = nil    
  mqtt_client.cb_onmessage = nil  
  mqtt_client.cb_publish = nil  
  mqtt_client.cb_subscribe = nil  
  mqtt_client.cb_unsubscribe = nil  


  
  

  mqtt_client.destroyed     = false
  mqtt_client.last_activity = 0
  mqtt_client.packet_id    = 0
  mqtt_client.outstanding   = {}
  

  
  mqtt_client.timer = tmr.create()
  mqtt_client.timer:register (5000, tmr.ALARM_AUTO, handler)	

  return(mqtt_client)
end


-- before connection
function MQTT.client.authenticate(                                       -- Public API
  username,     -- string:   Username
  password,     -- string:   Password
  secure)       -- integer:  Is this a secure session?
  
  self.username   = username
  self.password   = password
  self.secure     = secure
end  

-- before connection
function MQTT.client.lwt(                                       -- Public API
  topic,        -- string:   Will Topic
  message,      -- string:   Will Message
  qos,          -- integer:  QoS for Will
  retain)       -- integer:  Retain lwt

  self.lwt_topic = topic
  self.lwt_message = message
  self.lwt_qos = qos
  self.lwt_retain = retain  
end  





function MQTT.client:connection (sck, c)

  self.connected = true

-- Construct CONNECT variable header fields (bytes 1 through 9)
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  local payload
  payload = MQTT.client.encode_utf8("MQTT")
  payload = payload .. string.char(MQTT.VERSION)

-- Connect flags (byte 10)
-- ~~~~~~~~~~~~~
-- bit    7: Username flag =  0  -- recommended no more than 12 characters
-- bit    6: Password flag =  0  -- ditto
-- bit    5: Will retain   =  0
-- bits 4,3: Will QOS      = 00
-- bit    2: Will flag     =  0
-- bit    1: Clean session =  1
-- bit    0: Unused        =  0

  local flag
  flag = 0x00
  
  flag = flag + 2 -- Clean Session
  

  if (self.will_topic ~= nil) then
	flag = flag + 0x04 -- Will Flag
    flag = flag + MQTT.Utility.shift_left(self.will_retain, 5)
    flag = flag + MQTT.Utility.shift_left(self.will_qos, 3)
  end

  if (self.username != nil) then
    flag = flag + 0x80
  end
  
  if (self.password != nil) then
    flag = flag + 0x40
  end  
  

  payload = payload .. string.char(0x02)       -- Clean session, no last will


-- Keep alive timer (bytes 11 LSB and 12 MSB, unit is seconds)
-- ~~~~~~~~~~~~~~~~~
  payload = payload .. string.char(math.floor(self.keep_alive / 256))
  payload = payload .. string.char(self.keep_alive % 256)

-- Client identifier
-- ~~~~~~~~~~~~~~~~~
  payload = payload .. MQTT.client.encode_utf8(self.client_id)

-- Last will and testament
-- ~~~~~~~~~~~~~~~~~~~~~~~
  if (self.will_topic ~= nil) then
    payload = payload .. MQTT.client.encode_utf8(self.will_topic)
    payload = payload .. MQTT.client.encode_utf8(self.will_message)
  end

-- Username and Password
-- ~~~~~~~~~~~~~~~~~~~~~
  if (self.username != nil) then
    payload = payload .. MQTT.client.encode_utf8(self.username)
  end
  
  if (self.password != nil) then
    payload = payload .. MQTT.client.encode_utf8(self.password)
  end  

  local ret

  ret = self:message_write(MQTT.message.TYPE_CONNECT, payload)

  if (self.cb_connect ~= nil) then
    self.cb_connect
  end

-- Send MQTT message
-- ~~~~~~~~~~~~~~~~~
  return(ret)
end


function MQTT.client:connect()                                     -- Public API
                 -- return: nil or error message

  if (self.connected) then
    return("MQTT.client:connect(): Already connected")
  end

  MQTT.Utility.debug("MQTT.client:connect(): " .. identifier)

  self.net = net.createConnection (net.TCP, self.secure)
  self.net:on("connection", self:connection)
  self.net:on("receive", self:receive)
  self.net:on("disconnection", self.disconnection)
  
  -- -- ToDo: Implement the following functions
  -- 
  -- self.net:on("sent", self.sent)
  -- 
  -- self.net:on("reconnection", self.reconnection)  

  self.net:connect (self.hostname, self.port)

  if (self.net == nil) then
    return("MQTT.client:connect(): Couldn't open MQTT broker connection")
  end
end


function MQTT.client:disconnect()                                 -- Public API
  MQTT.Utility.debug("MQTT.client:disconnect()")

  if (self.connected) then
    self:message_write(MQTT.message.TYPE_DISCONNECT, nil)
    self.net:close()
    self.connected = false
    if (self.cb_onoffline ~= nil) then
      self.cb_onoffline
    end     
  else
    error("MQTT.client:disconnect(): Already disconnected")
  end
end




function MQTT.client:message_write(                             -- Internal API
  packet_type,  -- enumeration
  payload)       -- string
                 -- return: nil or error message

-- TODO: Complete implementation of fixed header byte 1

  local message = string.char(MQTT.Utility.shift_left(packet_type, 4))

  if (payload == nil) then
    message = message .. string.char(0)  -- Zero length, no payload
  else
    if (#payload > MQTT.client.MAX_PAYLOAD_LENGTH) then
      return(
        "MQTT.client:message_write(): Payload length = " .. #payload ..
        " exceeds maximum of " .. MQTT.client.MAX_PAYLOAD_LENGTH
      )
    end

    -- Encode "remaining length" (MQTT v3.1 specification pages 6 and 7)

    local remaining_length = #payload

    repeat
      local digit = remaining_length % 128
      remaining_length = math.floor(remaining_length / 128)
      if (remaining_length > 0) then digit = digit + 128 end -- continuation bit
      message = message .. string.char(digit)
    until remaining_length == 0

    message = message .. payload
  end

  self.net:send (message)

  self.last_activity = MQTT.Utility.get_time()
  return(nil)
end


function MQTT.client.encode_utf8(                               -- Internal API
  input)  -- string

  local output
  output = string.char(math.floor(#input / 256))
  output = output .. string.char(#input % 256)
  output = output .. input

  return(output)
end


function MQTT.client:receive (buffer, c)

    if (buffer ~= nil and #buffer > 0) then
      local index = 1

      -- Parse individual messages (each must be at least 2 bytes long)
      -- Decode "remaining length" (MQTT v3.1 specification pages 6 and 7)

      while (index < #buffer) do
        local packet_type_flags = string.byte(buffer, index)
        local multiplier = 1
        local remaining_length = 0

        repeat
          index = index + 1
          local digit = string.byte(buffer, index)
          remaining_length = remaining_length + ((digit % 128) * multiplier)
          multiplier = multiplier * 128
        until digit < 128                              -- check continuation bit

        local message = string.sub(buffer, index + 1, index + remaining_length)

        if (#message == remaining_length) then
          self:parse_message(packet_type_flags, remaining_length, message)
        else
          MQTT.Utility.debug(
            "MQTT.client:handler(): Incorrect remaining length: " ..
            remaining_length .. " ~= message length: " .. #message
          )
        end

        index = index + remaining_length + 1
      end

      -- Check for any left over bytes, i.e. partial message received

      if (index ~= (#buffer + 1)) then
        local error_message =
          "MQTT.client:handler(): Partial message received" ..
          index .. " ~= " .. (#buffer + 1)

        if (MQTT.ERROR_TERMINATE) then         -- TODO: Refactor duplicate code
          self:destroy()
          error(error_message)
        else
          MQTT.Utility.debug(error_message)
        end
      end
    end
  

  return(nil)
end

function MQTT.client:parse_message(                             -- Internal API
  packet_type_flags,  -- byte
  remaining_length,    -- integer
  message)             -- string: Optional variable header and payload

  local packet_type = MQTT.Utility.shift_right(packet_type_flags, 4)

-- TODO: MQTT.message.TYPE table should include "parser handler" function.
--       This would nicely collapse the if .. then .. elseif .. end.

  if (packet_type == MQTT.message.TYPE_CONACK) then
    self:parse_message_conack(packet_type_flags, remaining_length, message)

  elseif (packet_type == MQTT.message.TYPE_PUBLISH) then
    self:parse_message_publish(packet_type_flags, remaining_length, message)

  elseif (packet_type == MQTT.message.TYPE_PUBACK) then
    print("MQTT.client:parse_message(): PUBACK -- UNIMPLEMENTED --")    -- TODO

  elseif (packet_type == MQTT.message.TYPE_SUBACK) then
    self:parse_message_suback(packet_type_flags, remaining_length, message)

  elseif (packet_type == MQTT.message.TYPE_UNSUBACK) then
    self:parse_message_unsuback(packet_type_flags, remaining_length, message)

  elseif (packet_type == MQTT.message.TYPE_PINGREQ) then
    self:ping_response()

  elseif (packet_type == MQTT.message.TYPE_PINGRESP) then
    self:parse_message_pingresp(packet_type_flags, remaining_length, message)

  else
    local error_message =
      "MQTT.client:parse_message(): Unknown message type: " .. packet_type

    if (MQTT.ERROR_TERMINATE) then             -- TODO: Refactor duplicate code
      self:destroy()
      error(error_message)
    else
      MQTT.Utility.debug(error_message)
    end
  end
end

function MQTT.client:parse_message_conack(                      -- Internal API
  packet_type_flags,  -- byte
  remaining_length,    -- integer
  message)             -- string

  local me = "MQTT.client:parse_message_conack()"
  MQTT.Utility.debug(me)

  if (remaining_length ~= 2) then
    error(me .. ": Invalid remaining length")
  end

  local return_code = string.byte(message, 2)

  if (return_code ~= 0) then
    local error_message = "Unknown return code"

    if (return_code <= table.getn(MQTT.CONACK.error_message)) then
      error_message = MQTT.CONACK.error_message[return_code]
    end

    error(me .. ": Connection refused: " .. error_message)
    if (self:cb_onconnect ~= nil) then
      self:cb_noconnect ()
    end


  else
    if (self:cb_onconnect ~= nil) then
      self:cb_onconnect ()
    end
  end
  
end


function MQTT.client:disconnection()

  if (self.connected) then
      error("MQTT.client:disconnection(): Already connected")
  end

  if (self.cb_noconnect ~= nil) then
    self:noconnect 
  end

end











return MQTT
