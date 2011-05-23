require 'test_helper'

require File.dirname(__FILE__) + '/test_helper'

class CassandraMapperTest < ActiveSupport::TestCase
  
  def client
    $cassandra
  end
  # Replace this with your real tests.
  test "keyspace_created_correctly" do
    assert_equal "TestCassandraMapper", client.keyspace
  end
end
