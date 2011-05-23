module CassandraMapper

  class ColumnFamily
    
    def initialize(model_class)
      @model_class = model_class
    end
    
    %w(integer string boolean datetime).each do |type|
      define_method(type) do |*column_names|
        column_names.each do |name|
          @model_class.attribute_names << name.to_s
          @model_class.send(:define_method, name) do
            @attributes[name.to_s]
          end
          @model_class.send(:define_method, "#{name}=") do |value|
            @attributes_before_type_cast[name.to_s] = value
            @attributes[name.to_s] = CassandraMapper::Validators::Validator.get(type).type_cast(value)
          end
        end
      end
    end
    
    def timestamps
      datetime(:created_at, :updated_at)
    end
    
  end

  module ClassMethods
    
    def columns1(&block)
      CassandraMapper::ColumnFamily.new(self).instance_eval(&block)
    end
    
    attr_accessor :protected_attributes
    
    def attr_protected(*attributes)
      @protected_attributes ||= []
      @protected_attributes.push(*attributes) unless attributes.empty?
      @protected_attributes
    end
    
  end
end