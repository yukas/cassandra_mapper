module CassandraMapper
  module ActiveRecordAssociations
    
    def self.included(base)
      base.extend(ClassMethods)
    end
    
    module ClassMethods
      
      def has_many_cassandra(name, options = {})
        define_method(name) do |*scope_options|
          # TODO: understand more options besides :as
          if options.has_key?(:as)
            ActiveSupport::Inflector.classify(name).constantize.scoped(
              :conditions => "#{options[:as]}_id == #{attributes['id'].to_s} and #{options[:as]}_type == #{self.class.to_s}").scoped(*scope_options)
          else
            ActiveSupport::Inflector.classify(name).constantize.scoped(:conditions => "#{self.class.to_s.foreign_key} == #{self.id}")
          end
        end
      end
      
    end
  end
end