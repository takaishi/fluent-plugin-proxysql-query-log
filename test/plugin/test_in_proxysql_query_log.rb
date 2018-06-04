require "helper"
require "fluent/plugin/in_proxysql_query_log.rb"

class ProxysqlQueryLogInputTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
    Pathname.glob("#{TMP_DIR}/*").each{|p| File.delete(p)}
  end

  TMP_DIR = File.dirname(__FILE__) + '/../tmp/proxysql_query_log'
  CONFIG = config_element('ROOT', '', {
      'path' => "#{TMP_DIR}/query_log.00000001",
      'tag' => 't1',
  })

  MULTI_FILE_CONFIG = config_element('ROOT', '', {
      'path' => "#{TMP_DIR}/query_log*",
      'tag' => 't1',
  })

  QUERY_1 = {
      thread_id: 9,
      username: 'root',
      schema_name: 'alpaca',
      client: '127.0.0.1:34612',
      hid: 0,
      server: '127.0.0.1:3306',
      start_time: 1525944256367381,
      end_time: 1525944256367837,
      digest: '0xD69C6B36F32D2EAE',
      query: 'SELECT * FROM test'
  }

  QUERY_2 = {
      thread_id: 9,
      username: 'root',
      schema_name: 'alpaca',
      client: '127.0.0.1:34612',
      hid: 0,
      server: '127.0.0.1:3306',
      start_time: 1525944256367381,
      end_time: 1525944256367837,
      digest: '0xD69C6B36F32D2EAE',
      query: 'show databases'
  }

  test 'singlefile' do
    File.open("#{TMP_DIR}/query_log.00000001", 'wb') {|f|
      write_record(f, QUERY_1)
    }
    config = CONFIG
    d = create_driver(config)
    d.run(expect_emits: 1) do
      File.open("#{TMP_DIR}/query_log.00000001", "ab") {|f|
        write_record(f, QUERY_1)
      }
    end

    events = d.events
    assert_equal(1, d.instance.instance_variable_get('@watchers').size)
    assert_equal(true, events.length > 0)
    assert_equal(1, 1)
    assert_equal(9, events[0][2]['thread_id'])
    assert_equal('root', events[0][2]['username'])
    assert_equal('alpaca', events[0][2]['schema_name'])
    assert_equal('127.0.0.1:34612', events[0][2]['client'])
    assert_equal(0, events[0][2]['HID'])
    assert_equal('127.0.0.1:3306', events[0][2]['server'])
    assert_equal('2018-05-10 09:24:16', events[0][2]['start_time'])
    assert_equal('2018-05-10 09:24:16', events[0][2]['end_time'])
    assert_equal('0xD69C6B36F32D2EAE', events[0][2]['digest'])
    assert_equal('SELECT * FROM test', events[0][2]['query'])
  end

  test 'multifile' do
    File.open("#{TMP_DIR}/query_log.00000001", 'wb') {|f|
      write_record(f, QUERY_1)
    }
    File.open("#{TMP_DIR}/query_log.00000002", 'wb') {|f|
      write_record(f, QUERY_2)
    }

    config = MULTI_FILE_CONFIG
    d = create_driver(config)
    d.run(expect_emits: 2) do
      File.open("#{TMP_DIR}/query_log.00000001", "ab") {|f|
        write_record(f, QUERY_1)
      }
      File.open("#{TMP_DIR}/query_log.00000002", "ab") {|f|
        write_record(f, QUERY_2)
      }
    end

    events = d.events
    assert_equal(2, d.instance.instance_variable_get('@watchers').size)
    assert_equal(true, events.length > 0)
    assert_equal(1, 1)
    assert_equal(9, events[0][2]['thread_id'])
    assert_equal('root', events[0][2]['username'])
    assert_equal('alpaca', events[0][2]['schema_name'])
    assert_equal('127.0.0.1:34612', events[0][2]['client'])
    assert_equal(0, events[0][2]['HID'])
    assert_equal('127.0.0.1:3306', events[0][2]['server'])
    assert_equal('2018-05-10 09:24:16', events[0][2]['start_time'])
    assert_equal('2018-05-10 09:24:16', events[0][2]['end_time'])
    assert_equal('0xD69C6B36F32D2EAE', events[0][2]['digest'])
    assert_equal('SELECT * FROM test', events[0][2]['query'])

    assert_equal(2, d.instance.instance_variable_get('@watchers').size)
    assert_equal(true, events.length > 0)
    assert_equal(1, 1)
    assert_equal(9, events[1][2]['thread_id'])
    assert_equal('root', events[1][2]['username'])
    assert_equal('alpaca', events[1][2]['schema_name'])
    assert_equal('127.0.0.1:34612', events[1][2]['client'])
    assert_equal(0, events[1][2]['HID'])
    assert_equal('127.0.0.1:3306', events[1][2]['server'])
    assert_equal('2018-05-10 09:24:16', events[1][2]['start_time'])
    assert_equal('2018-05-10 09:24:16', events[1][2]['end_time'])
    assert_equal('0xD69C6B36F32D2EAE', events[1][2]['digest'])
    assert_equal('show databases', events[1][2]['query'])
  end

  private

  def write_record(f, param)
    q = ProxysqlQueryLog::Query.new
    q.thread_id = param[:thread_id]
    q.username = param[:username]
    q.schema_name = param[:schema_name]
    q.client = param[:client]
    q.hid = param[:hid]
    q.server = param[:server]
    q.start_time = param[:start_time]
    q.end_time = param[:end_time]
    q.digest = param[:digest]
    q.query = param[:query]

    f.write([total_length(q), 0, 0, 0, 0, 0, 0, 0].pack('C*'))
    f.write(to_binary(q))
  end

  def create_driver(conf)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::ProxysqlQueryLogInput).configure(conf)
  end
end
