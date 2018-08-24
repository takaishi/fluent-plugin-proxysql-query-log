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
require 'fluent/event'
require 'fluent/plugin/in_proxysql_query_log/watcher'

module Fluent
  module Plugin
    class ProxysqlQueryLogInput < Fluent::Plugin::Input
      Fluent::Plugin.register_input("proxysql_query_log", self)

      helpers :event_loop, :storage, :timer

      DEFAULT_STORAGE_TYPE = 'local'

      config_param :path, :string
      config_param :read_from_head, :bool, default: false
      desc 'The paths to exclude the files from watcher list.'
      config_param :exclude_path, :array, default: []
      config_param :refresh_interval, :time, default: 60
      config_param :tag, :string

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

        refresh_watchers
        timer_execute(:in_proxysql_query_log_refresh_watchers, @refresh_interval, &method(:refresh_watchers))
      end

      def refresh_watchers
        target_paths = expand_paths
        remove_detached_watcher
        start_watchers(target_paths)
      end

      def start_watchers(paths)

        paths.each do |path|
          unless @watchers.has_key?(path)
            log.debug("start watch: #{path}")
            @watchers[path] = Watcher.new(path, 0, @pos_storage, router, @tag, log)
            event_loop_attach(@watchers[path])
          end
        end
      end

      def remove_detached_watcher
        @watchers = @watchers.select { |k, v| v.attached? }
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
