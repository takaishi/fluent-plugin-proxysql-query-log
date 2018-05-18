#
# Copyright 2018- r_takaishi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require "fluent/plugin/input"
require 'proxysql_query_log/parser'

module Fluent
  module Plugin
    class ProxysqlQueryLogInput < Fluent::Plugin::Input
      Fluent::Plugin.register_input("proxysql_query_log", self)

      helpers :storage

      DEFAULT_STORAGE_TYPE = 'local'

      config_param :path, :string
      config_param :read_from_head, :bool, default: false

      config_section :storage do
        config_set_default :usage, 'positions'
        config_set_default :@type, DEFAULT_STORAGE_TYPE
        config_set_default :persistent, false
      end

      def configure(conf)
        super
        @pos_storage = storage_create(usage: 'positions', type: DEFAULT_STORAGE_TYPE, conf: conf)
      end

      def start
        super

        Signal.trap('INT', 'TERM') do |signo|
          @io.close
          exit 0
        end

        parser = ProxysqlQueryLog::Parser.new

        @io = File.open(@path)
        seek

        while true
          raw_total_bytes = @io.read(1)
          next unless raw_total_bytes

          total_bytes = raw_total_bytes.unpack('C')[0]
          @io.seek(7, IO::SEEK_CUR)
          raw = @io.read(total_bytes)
          query = parser.parse(StringIO.new(raw, 'r+'))
          record = {
              'thread_id' => query.thread_id,
              'username' => query.username,
              'schema_name' => query.schema_name,
              'client' => query.client,
              'HID' => query.hid,
              'server' => query.server,
              'start_time' => Time.at(query.start_time/1000/1000),
              'end_time' => Time.at(query.end_time/1000/1000),
              'duration' => query.end_time - query.start_time,
              'digest' => query.digest,
              'query' => query.query
          }
          router.emit('', query.start_time/1000/1000, record)
          @pos_storage.put(:journal, @io.pos)
        end
      end

      private

      def seek
        cursor = @pos_storage.get(:journal)
        if cursor
          @io.seek(cursor, IO::SEEK_SET)
        end
      end
    end
  end
end
