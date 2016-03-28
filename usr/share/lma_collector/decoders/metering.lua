-- Copyright 2015 Mirantis, Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
require "string"
require "cjson"
require 'table'

local patt = require 'patterns'
local utils = require 'lma_utils'
local l = require 'lpeg'
l.locale(l)

function normalize_uuid(uuid)
    return patt.Uuid:match(uuid)
end

-- the tag_fields parameter is a list of tags separated by spaces
local fields_grammar = l.Ct((l.C((l.P(1) - l.P" ")^1) * l.P" "^0)^0)
local metadata_fields = fields_grammar:match(
    read_config("metadata_fields") or ""
)

local sample_msg = {
    Timestamp = nil,
    Type = "sample.bulk_metric",
    Payload = nil
}

local meter_msg = {
    Timestamp = nil,
    Type = "resource",
    Fields = nil,
}

local resource_msg = {
    Timestamp = nil,
    Type = "resource",
    Fields = nil,
}

function inject_metadata(metadata, tags)
    local value
    for _, field in ipairs(metadata_fields) do
        value = metadata[field]
        if value ~= nil and type(value) ~= 'table' then
            tags["metadata." .. field] = value
        end
    end
end

function inject_resource_metadata(metadata, fields)
    for field, value in pairs(metadata) do
        if field ~= "OS-EXT-AZ:availability_zone" then
            if value and type(value) ~= 'table' and value ~= nil then
                fields[field] = value
            end
        end
    end
end


function inject_resource(sample)
    resource_msg.Fields = {
        resource_id = sample.resource_id,
        source = sample.source or "",
        last_sample_timestamp = sample.timestamp
    }
    local payload = {
        resource_id = sample.resource_id,
        source = sample.source or "",
        last_sample_timestamp = sample.timestamp,
        metadata = sample.resource_metadata
    }

    resource_msg.Timestamp = sample.timestamp
    resource_msg.Payload = cjson.encode(payload)
    utils.safe_inject_message(resource_msg)

    payload = {
        user_id = sample.user_id or "",
        project_id = sample.project_id or "",
        resource_id = sample.resource_id,
        source = sample.source or "",
        meter_name = sample.counter_name,
        meter_type = sample.counter_type,
        meter_unit = sample.counter_unit or "",
        metadata = sample.resource_metadata
    }
    meter_msg.Fields = {
        resource_id = sample.resource_id,
        meter_name = sample.counter_name,
    }

    utils.safe_inject_message(meter_msg)
end


function inject_sample_to_payload(sample, payload)
    local sample_data = {
        name='sample',
        timestamp = patt.Timestamp:match(sample.timestamp),
        value = {
            value = sample.counter_volume,
            message_id = sample.message_id,
            recorded_at = sample.recorded_at,
            timestamp = sample.timestamp,
            message_signature = sample.signature,
            type = sample.counter_type,
            unit = sample.counter_unit
        }
    }
    local tags = {
        meter = sample.counter_name,
        resource_id = sample.resource_id,
        project_id = sample.project_id ,
        user_id = sample.user_id,
        source = sample.source
    }
    inject_metadata(sample.resource_metadata or {}, tags)
    sample_data["tags"] = tags
    table.insert(payload, sample_data)
end

function process_message ()
    local data = read_message("Payload")
    local ok, message = pcall(cjson.decode, data)
    if not ok then
        return -1
    end
    local ok, message_body = pcall(cjson.decode, message["oslo.message"])
    if not ok then
        return -1
    end
    local payload = {}
    for _, sample in ipairs(message_body["payload"]) do
        inject_sample_to_payload(sample, payload)
        inject_resource(sample)
    end
    sample_msg.Payload = cjson.encode(payload)
    sample_msg.Timestamp = patt.Timestamp:match(message_body.timestamp)
    inject_message(sample_msg)

    return 0
end
