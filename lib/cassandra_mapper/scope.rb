module CassandraMapper
  class Scope

    attr_accessor :conditions, :order, :limit
    
    def initialize(model_class)
      @model_class = model_class
      @conditions = []
    end
    
    def apply_scope_options(options = {})
      if options != nil
        @conditions += conditions_from_string(options[:conditions]) if options.has_key?(:conditions)
        @order = options[:order] if options.has_key?(:order)
        @limit = options[:limit] if options.has_key?(:limit)
      end
      self
    end
    
    def scoped(options = {})
      apply_scope_options(options)
    end
    
    def all(scope_options = nil)
      if scope_options
        scoped(scope_options).all
      else
        find
      end
    end
    
    def count
      find.count
    end
    
    def create(attributes = {})
      build(attributes).save
    end
    
    def build(attributes = {})
      for condition in conditions
        if condition.last == '=='
          attributes.merge!({condition[0] => condition[1]})
        end
      end
      new_record = @model_class.new(attributes)
      yield(new_record) if block_given?
      new_record
    end
    
    def method_missing(method_name, *args)
      if @model_class.scopes.has_key?(method_name)
        merge(@model_class.send(method_name, *args))
      else
        super
      end
    end
    
    private
    
    def merge(other)
      # TODO: correctly merge conditions
      @conditions += other.conditions
      @order = other.order
      @limit = other.limit
      self
    end
    
    def find
      idx_clause_params = []
      @conditions.each do |param|
        idx_clause_params << @model_class.connection.create_idx_expr(*param)
      end
      idx_clause = @model_class.connection.create_idx_clause(idx_clause_params)
      indexed_row = @model_class.connection.get_indexed_slices(@model_class.column_family_name, idx_clause)

      result = []
      indexed_row.each do |key, columns|
        new_record = @model_class.new(columns.to_attributes)
        new_record.key = key
        result << new_record
      end
      
      if @order.blank?
        result
      else
        attribute, order = @order.split(" ")
        result = result.sort_by { |record| record.attributes[attribute] }
        result = result.reverse if order != nil and order.strip == "DESC"
        result
      end
    end
    
    def conditions_from_string(conditions_string)
      condition_parts = conditions_string.split("and")
      condition_parts.map do |condition|
        parts = condition.split(/(>|<|==)/).map do |part|
          part.strip
        end
        temp = parts[2]
        parts[2] = parts[1]
        parts[1] = temp
        parts
      end
    end
    
  end
  
end