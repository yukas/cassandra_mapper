module CassandraMapper
  
  def self.included(base)
    base.extend ClassMethods
    base.extend ::ActiveModel::Naming
  end
  
  module ClassMethods
    
    # def base_class
    #   Comment
    # end
    # 
    # Connection
    
    def connection
      $cassandra
    end
    
    def establish_connection(*spec)
      $cassandra = Cassandra.new(*spec)
    end
    
    # ActiveRecord interface
    
    def attribute_names
      @attribute_names ||= []
    end
    
    def connected?
      connection.nil?
    end
    
    def build(attributes)
      new(attributes)
    end
    
    def create(attributes = {}, &block)
      new_record = new(attributes)
      yield(new_record) if block_given?
      new_record.save
    end
    
    def update(key, attributes)
      connection.insert(column_family_name, key, attributes)
    end
    
    def update_all(attributes, conditions)
      raise "Implement me, please!"
    end

    def get(key)
      key = key.to_s
      new_record = new(connection.get(column_family_name, key))
      new_record.key = key
      new_record
    end
    
    def all
      connection.get_range(column_family_name).map do |key_slice|
        new_record = new(key_slice.to_ordered_hash)
        new_record.key = key_slice.key
        new_record
      end
    end
    
    def delete(*keys)
      for key in keys
        connection.remove(column_family_name, key)
      end
    end
    
    def delete_all(some_kind_of_find_options)
      raise "Implement me, please!"
    end
    
    def truncate!
      connection.truncate!(column_family_name)
    end
    
    def destroy(*keys)
      for key in keys
        get(key).delete
      end
    end
    
    def destroy_all(some_kind_of_find_options)
      raise "Implement me, please!"
    end
    
    def exists?(key)
      connection.exists?(column_family_name, key)
    end
    
    # Helpers

    def column_family_name
      self.to_s #.pluralize
    end
    
    def generate_key
      SimpleUUID::UUID.new.to_i.to_s
    end
    
    # Indexes
    
    def index(name)
      # TODO: Select appropriate type
      connection.create_index("Naharnet", column_family_name, name.to_s, "IntegerType")
    end
    
    def drop_index(attribute)
      connection.drop_index("Naharnet", column_family_name, attribute.to_s)
    end
    
    def find(options = {})
      case options
      when Hash
        scoped(options)        
      when Integer
        get(options)
      end
    end
    
    # Scopes
    
    def scopes
      @scopes ||= {}
    end
    
    def scoped(options = nil)
      if options
        scoped.apply_scope_options(options)
      else
        Scope.new(self)
      end
    end
    
    def scope(name, scope_options = {})
      scopes[name] = lambda do |*args|
        scoped(scope_options).scoped(*args)
      end
      singleton_class.send(:define_method, name, &scopes[name])
    end
    
    def singleton_class
      class << self; self; end
    end
    
    # Associations
    
    attr_accessor :associations

    def belongs_to(name, options)
      @associations ||= []
      @associations << [name, options]
    end

  end
end