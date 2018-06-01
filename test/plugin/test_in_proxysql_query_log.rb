require "helper"
require "fluent/plugin/in_proxysql_query_log.rb"

class ProxysqlQueryLogInputTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
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

  test 'singlefile' do
    File.open("#{TMP_DIR}/query_log.00000001", 'wb') {|f|
      write_record(f)
    }
    config = CONFIG
    d = create_driver(config)
    d.run(expect_emits: 1) do
      File.open("#{TMP_DIR}/query_log.00000001", "ab") {|f|
        write_record(f)
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
      write_record(f)
    }
    File.open("#{TMP_DIR}/query_log.00000002", 'wb') {|f|
      write_record(f)
    }

    config = MULTI_FILE_CONFIG
    d = create_driver(config)
    d.run(expect_emits: 2) do
      File.open("#{TMP_DIR}/query_log.00000001", "ab") {|f|
        write_record(f)
      }
      File.open("#{TMP_DIR}/query_log.00000002", "ab") {|f|
        write_record(f)
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
  end

  private

  def write_record(f)
    q = ProxysqlQueryLog::Query.new
    q.thread_id = 9
    q.username = 'root'
    q.schema_name = 'alpaca'
    q.client = '127.0.0.1:34612'
    q.hid = 0
    q.server = '127.0.0.1:3306'
    q.start_time = 1525944256367381
    q.end_time = 1525944256367837
    q.digest = '0xD69C6B36F32D2EAE'
    q.query = 'SELECT * FROM test'

    f.write([total_length(q), 0, 0, 0, 0, 0, 0, 0].pack('C*'))
    f.write(to_binary(q))
  end

  def create_driver(conf)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::ProxysqlQueryLogInput).configure(conf)
  end
end
