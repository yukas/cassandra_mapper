require "active_record"
require "cassandra_mapper/cassandra_thrift_extensions.rb"
require "cassandra_mapper/core_extensions.rb"

module CassandraMapper
  
  attr_accessor :key, :attributes
  alias_method :id, :key
  
  def key=(key)
    @key = key.to_s
  end
  
  def attributes=(new_attributes)
    new_attributes.each do |name, value|
      unless self.class.protected_attributes.include?(name.to_sym)
        load_attributes name => value
      end
    end
    @attributes
  end
  
  def initialize(attributes = {})
    @column_family_name = self.class.column_family_name
    
    @attributes, @attributes_before_type_cast = {}, {}
    load_attributes(attributes)
    
    @readonly = false
  end
  
  # ActiveModel integration
  
  include ::ActiveModel::Conversion
  
  def persisted?
    !new_record?
  end

  def to_key
    [@key]
  end
  
  # ActiveRecord interface
  
  def increment(name, by = 1)
    load_attributes name => @attributes[name.to_s].to_i + by
    self
  end
  
  def increment!(name, by = 1)
    increment(name, by)
    save
  end
  
  def decrement(name, by = 1)
    load_attributes name => @attributes[name.to_s].to_i - by
    self
  end
  
  def decrement!(name, by = 1)
    decrement(name, by)
    save
  end
  
  def column_for_attribute
    raise "Implement me, please!"
  end
  
  def connection
    self.class.connection
  end
  
  def clone
    self.class.new(@attributes)
  end
  
  def cache_key
    result = "#{self.class.column_family_name}/"
    result << (new_record? ? "new" : "#{@key}")
    result << "-#{@attributes[:updated_at].to_i}" if column_present? :updated_at
    result
  end
  
  def attributes_before_type_cast
    @attributes_before_type_cast.dup
  end
  
  def attribute_present?(attribute)
    @attributes[attribute.to_sym].blank? ? false : true
  end
  
  def has_attribute?(name)
    self.class.attribute_names.include?(name.to_s)
  end
  
  def new_record?
    @key.nil?
  end
  
  def eql?(other)
    self == other
  end
  
  def hash
    @key
  end

  def freeze
    @attributes.freeze
    self
  end
  
  def frozen?
    @attributes.frozen?
  end
  
  def readonly!
    @readonly = true
  end
  
  def readonly?
    @readonly
  end
  
  def inspect
    result = "#{@column_family_name} key:#{@key.nil? ? 'nil' : @key.inspect} columns: { "
    result << @attributes.map { |name, value| "#{name} => #{value.inspect}" }.join(', ') << ' }'
  end
  
  def update_attribute(name, value)
    load_attributes(name.to_sym => value)
    save
  end
  
  def update_attributes(attributes)
    load_attributes(attributes)
    save
  end
  
  def update_attributes!(attributes)
    load_attributes(attributes)
    save!
  end
  
  def toggle(attr_name)
    @attributes[attr_name.to_sym] = !@attributes[attr_name.to_sym]
    self
  end
  
  def toggle!(attr_name)
    toggle(attr_name)
    save
  end
  
  def reload(options = nil)
    load_attributes(connection.get(@column_family_name, @key))
    self
  end
  
  def save
    update_timestamps(new_record?)
    @key ||= generate_key
    connection.insert(@column_family_name, @key.to_s, @attributes.stringnify_values)
    self
  end
  
  def save!
    save
  end
  
  def delete
    connection.remove(@column_family_name, @key)
    freeze
  end
  
  def destroy
    delete
  end
  
  def method_missing(method_name, *args)
    associations.each do |association|
      if association.first == method_name
        name, options = *association
        if options[:model] == :active_record
          if options[:polymorphic]
            model_class_name = @attributes["#{name.to_s}_type"]
          else
            model_class_name = name.to_s.capitalize
          end

          model_instance_id = @attributes["#{name.to_s}_id"].to_i
          
          self.class.send(:define_method, name) do
            if !model_class_name.nil? and !model_instance_id.nil?
              model_class_name.constantize.find(model_instance_id) rescue ActiveRecord::RecordNotFound
            end
          end
          
          return send(name)
        end
      end
    end
    super
  end
  
  protected
  
  def update_timestamps(both = true)
    load_attributes :updated_at => now = DateTime.now
    load_attributes :created_at => now if both
  end
  
  def load_attributes(attributes = {})
    attributes.each do |name, value|
      if has_attribute?(name)
        send("#{name}=", value) 
      else
        raise "Did not have column '#{name}'"
      end
    end
  end
  
  # Helpers
  
  def generate_key
    self.class.generate_key
  end
  
  def associations
    self.class.associations
  end

end

require "cassandra_mapper/validators.rb"
require "cassandra_mapper/attributes.rb"

require "cassandra_mapper/class_methods.rb"
require "cassandra_mapper/scope.rb"

require "cassandra_mapper/active_record_extensions.rb"