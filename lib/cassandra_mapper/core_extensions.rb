class Hash
  def stringnify_values
    result = {}
    self.each do |k,v|
      result[k] = v.to_s
    end
    result
  end
end

class Array
  def to_attributes
    result = {}
    each do |column|
      result[column.column.name] = column.column.value
    end
    result
  end
end