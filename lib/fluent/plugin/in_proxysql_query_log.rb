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

      config_param :path, :string
      config_param :read_from_head, :bool, default: false

      def start
        super
        parser = ProxysqlQueryLog::Parser.new

        parser.load_file(@path).each do |query|
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
              'digest' => query.digest
          }
          router.emit('', query.start_time, record)
        end

      end
    end
  end
end
