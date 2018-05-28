require "helper"
require "fluent/plugin/in_proxysql_query_log.rb"

class ProxysqlQueryLogInputTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
  end

  TMP_DIR = File.dirname(__FILE__) + '/../tmp/proxysql_query_log'
  CONFIG = config_element('ROOT', '', {
      'path' => "#{TMP_DIR}/query_log"
  })

  test 'singlefile' do
    File.open("#{TMP_DIR}/query_log", 'wb') {|f|
      write_record(f)
    }
    config = CONFIG
    d = create_driver(config)
    d.run(expect_emits: 1) do
      File.open("#{TMP_DIR}/query_log", "ab") {|f|
        write_record(f)
      }
    end

    events = d.events
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
    f.write([92, 0, 0, 0, 0, 0, 0, 0].pack('C*'))
    f.write([0].pack('C*'))

    f.write([9].pack('C*'))

    f.write([4].pack('C*'))
    f.write('root')

    f.write([6].pack('C*'))
    f.write('alpaca')

    f.write([15].pack('C*'))
    f.write('127.0.0.1:34612')

    f.write([0].pack('C*'))

    f.write([14].pack('C*'))
    f.write('127.0.0.1:3306')

    f.write([0xfe].pack('C*'))
    f.write([1525944256367381].pack('Q*'))

    f.write([0xfe].pack('C*'))
    f.write([1525944256367837].pack('Q*'))

    f.write([0xfe].pack('C*'))
    f.write('0xD69C6B36F32D2EAE'.gsub(/0x/, '').scan(/.{1,8}/).map{|s| s.hex}.pack('I*'))

    f.write([18].pack('C*'))
    f.write('SELECT * FROM test')
  end

  def create_driver(conf)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::ProxysqlQueryLogInput).configure(conf)
  end
end
