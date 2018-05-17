require "helper"
require "fluent/plugin/in_proxysql_query_log.rb"

class ProxysqlQueryLogInputTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
  end

  test "failure" do
    flunk
  end

  private

  def create_driver(conf)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::ProxysqlQueryLogInput).configure(conf)
  end
end
