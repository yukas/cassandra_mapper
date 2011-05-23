module CassandraThrift
  class KeySlice
    def to_ordered_hash
      h = ::Cassandra::OrderedHash.new
      columns.each do |c|
        h[c.column.name] = c.column.value
      end
      h
    end
  end
end