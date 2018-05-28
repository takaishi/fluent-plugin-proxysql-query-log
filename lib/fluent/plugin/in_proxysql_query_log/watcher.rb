module Fluent
  module Plugin
    class ProxysqlQueryLogInput < Fluent::Plugin::Input
      class Watcher < Coolio::StatWatcher
        def initialize(path, interval, pos_storage, router, log)
          super(path, interval)

          @parser = ProxysqlQueryLog::Parser.new
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
          @io = File.open(@path)
          seek(@path)

          while raw_total_bytes = @io.read(1)
            return unless raw_total_bytes

            total_bytes = raw_total_bytes.unpack('C')[0]
            @io.seek(7, IO::SEEK_CUR)
            raw = @io.read(total_bytes)
            query = @parser.parse(StringIO.new(raw, 'r+'))
            @router.emit('', query.start_time/1000/1000, record(query))
            @pos_storage.put(@path, @io.pos)
          end
        end

        def record(query)
          {
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
        end
      end
    end
  end
end