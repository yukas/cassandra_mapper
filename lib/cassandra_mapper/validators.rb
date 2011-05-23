module CassandraMapper
  module Validators
    
    class Validator
      
      def self.get(type)
        case type
        when 'string'
          StringValidator.new
        when 'integer'
          IntegerValidator.new
        when 'boolean'
          BooleanValidator.new
        when 'datetime'
          DateTimeValidator.new
        else
          Validator.new
        end
      end
      
      def type_cast(value)
        value
      end
      
    end

    class IntegerValidator < Validator
      def type_cast(value)
        value.to_i
      end
    end

    class StringValidator < Validator
      def type_cast(value)
        value.to_s
      end
    end

    class BooleanValidator < Validator
      def type_cast(value)
        !!value
      end
    end

    class DateTimeValidator < Validator
      def type_cast(value)
        value.is_a?(DateTime) ? value : DateTime.parse(value)
      end
    end

  end

end