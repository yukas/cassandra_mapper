require 'rubygems'
require 'test/unit'
require 'active_support'

require 'cassandra'

cfg = YAML.load_file("#{File.dirname(__FILE__)}/cassandra.yml")['test']
thrift_options = { :timeout => cfg['timeout'], :retries => cfg['retries'] }
$cassandra = Cassandra.new(cfg['keyspace'], cfg['servers'], thrift_options)
$cassandra.disable_node_auto_discovery!