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

-- TODO: Write a resource processing
require 'cjson'
require 'os'
require 'string'
require 'table'
local field_util = require 'field_util'
local utils = require 'lma_utils'
local l = require 'lpeg'
l.locale(l)


local ts_from_message = read_config("es_index_from_timestamp")
local index = read_config("index") or "heka-%{%Y.%m.%d}"
local type_name = read_config("type_name") or "message"
local id = read_config("id")

local flush_count = read_config('flush_count') or 100
local flush_interval = read_config('flush_interval') or 5
local default_tenant_id = read_config("default_tenant_id")
local default_user_id = read_config("default_user_id")
local time_precision = read_config("time_precision")
local payload_name = read_config("payload_name") or "resources"
-- the tag_fields parameter is a list of tags separated by spaces

local defaults = {
    tenant_id=default_tenant_id,
    user_id=default_user_id,
}
local last_flush = os.time()
local resources = {}

-- Flush the datapoints to InfluxDB if enough items are present or if the
-- timeout has expired
function flush ()
    local now = os.time()
    if #resources > 0 and (#resources > flush_count or now - last_flush > flush_interval) then
        -- resources[] = ''
        local bulk_requests = format_to_bulk(resources)
        utils.safe_inject_payload("txt", "elasticsearch", table.concat(bulk_requests, "\n"))

        resources = {}
        last_flush = now
    end
end

function format_to_bulk(resources)
    local es_meta
    local bulk_table = {}
    for resource, data in ipairs(resources):
        es_meta = cjson.decode(elasticsearch.bulkapi_index_json(index, type_name, id, ns))
         
         -- change type of request from index to the update
         -- {"update":{"_index":"mylogger-2014.06.05","_type":"mytype-host.domain.com"}}
         -- {script: ..., params: {...}, upsert: {...}}
        es_meta = cjson.encode({update = idx_json['index']})
        local body = {
            script = 'ctx._source.meters += meters; ctx._source.user_id = user_id;' ..
                     'ctx._source.project_id = project_id;' ..
                     'ctx._source.source = source;' ..
                     'ctx._source.metadata = ' ..
                     'ctx._source.last_sample_timestamp <= timestamp ? ' ..
                     'metadata : ctx._source.metadata;' ..
                     'ctx._source.last_sample_timestamp = ' ..
                     'ctx._source.last_sample_timestamp < timestamp ?' ..
                     'timestamp : ctx._source.last_sample_timestamp;' ..
                     'ctx._source.first_sample_timestamp = ' ..
                     'ctx._source.first_sample_timestamp > timestamp ?' ..
                     'timestamp : ctx._source.first_sample_timestamp;',
            params = {
                meters = resource.meters,
                metadata = resource.metadata,
                first_timestamp = resource.first_timestamp,
                last_timestamp = resource.last_timestamp,
                user_id = resource.user_id,
                project_id = resource.project_id,
                source = resource.source,
            },
            upsert = {
                first_timestamp = resource.first_timestamp,
                last_timestamp = resource.last_timestamp,
                user_id = resource.user_id,
                project_id = resource.project_id,
                metadata = resource.metadata,
                meters = resource.meters
            }
    }

end

-- Return the Payload field decoded as JSON data, nil if the payload isn't a
-- valid JSON string
function decode_json_payload()
    local ok, data = pcall(cjson.decode, read_message("Payload"))
    if not ok then
        return
    end

    return data
end

function process_resources()
    -- The payload contains a list of datapoints, each point being formatted
    -- like this: {name='foo',value=1,tags={k1=v1,...}}
    local resources_table = decode_json_payload()
    if not resources_table then
        return 'Invalid payload value'
    end
    local meter
    local ts
    for resource_id, resource_info in ipairs(datapoints) do
        meter = resource_info.meter
        ts = resource_info.timestamp
        table.remove(resource_info, "meter")
        table.remove(resource_info, "timestamp")
        resources[resource_id] = resource_info
        for meter_name, meter_info in ipairs(meter) do
            resources[resource_id][meters][meter_name] = meter_info
        end
    end
end

function process_message()
    local err_msg
    process_resources()

    flush()

    if err_msg then
        return -1, err_msg
    else
        return 0
    end
end

function timer_event(ns)
    flush()
end
