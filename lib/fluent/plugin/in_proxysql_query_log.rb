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

require 'cool.io'
require 'proxysql_query_log/parser'

require 'fluent/plugin/input'
require 'fluent/plugin/in_proxysql_query_log/watcher'

module Fluent
  module Plugin
    class ProxysqlQueryLogInput < Fluent::Plugin::Input
      Fluent::Plugin.register_input("proxysql_query_log", self)

      helpers :storage

      DEFAULT_STORAGE_TYPE = 'local'

      config_param :path, :string
      config_param :read_from_head, :bool, default: false
      desc 'The paths to exclude the files from watcher list.'
      config_param :exclude_path, :array, default: []

      config_section :storage do
        config_set_default :usage, 'positions'
        config_set_default :@type, DEFAULT_STORAGE_TYPE
        config_set_default :persistent, false
      end

      def initialize
        super
        @paths = []
        @watchers = {}
      end

      def configure(conf)
        super
        @pos_storage = storage_create(usage: 'positions', type: DEFAULT_STORAGE_TYPE, conf: conf)
        @paths = @path.split(',').map{|path|path.strip}
      end

      def start
        super

        Signal.trap(:INT, 'EXIT') do |signo|
          shutdown
        end

        Signal.trap(:TERM, 'EXIT') do |signo|
          shutdown
        end

        target_paths = expand_paths
        start_watchers(target_paths)
      end

      def start_watchers(paths)
        reactor = Coolio::Loop.new

        paths.each do |path|
          log.debug("start watch: #{path}")
          w = Watcher.new(path, 0, @pos_storage, router, log)
          reactor.attach(w)
          @watchers[path] = w
        end
        reactor.run
      end

      def shutdown
        @io.close unless @io.closed?

        super
      end

      private

      def expand_paths
        @paths.map do |path|
          if path.include?('*')
            Dir.glob(path)
          else
            path
          end
        end.flatten
      end
    end
  end
end
