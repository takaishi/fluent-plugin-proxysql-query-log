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

        if @pos_file
          @pf_file = File.open(@pos_file, File::RDWR|File::CREAT|File::BINARY, @file_perm)
          @pf_file.sync = true
          @pf = PositionFile.parse(@pf_file)
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

    class Watcher < Coolio::StatWatcher
      def initialize(path, interval, pos_storage, router, log)
        super(path, interval)

        @pos_storage = pos_storage
        @router = router
        @log = log
        read
      end

      def seek(path)
        cursor = @pos_storage.get(path)
        if cursor
          @io.seek(cursor, IO::SEEK_SET)
        end
      end

      def on_change(previous, current)
        @log.debug ("change: #{@path}")
        @log.debug ("previous: #{previous}")
        @log.debug ("current: #{current}")
        read
      end

      def read
        parser = ProxysqlQueryLog::Parser.new
        @io = File.open(@path)
        seek(@path)

        while
        raw_total_bytes = @io.read(1)
          return unless raw_total_bytes

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
              'start_time' => Time.at(query.start_time/1000/1000).utc.strftime('%Y-%m-%d %H:%M:%S'),
              'end_time' => Time.at(query.end_time/1000/1000).utc.strftime('%Y-%m-%d %H:%M:%S'),
              'duration' => query.end_time - query.start_time,
              'digest' => query.digest,
              'query' => query.query
          }
          @router.emit('', query.start_time/1000/1000, record)
          @pos_storage.put(@path, @io.pos)
        end
      end
    end
  end
end
