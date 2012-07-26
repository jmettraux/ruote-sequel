
#
# testing ruote-sequel
#
# Thu Feb 10 11:14:56 JST 2011
#

require 'rufus-json/automatic'
require 'ruote-sequel'


unless $sequel

  $sequel = case ENV['RUOTE_STORAGE_DB'] || 'postgres'
    when 'pg', 'postgres'
      Sequel.connect('postgres://localhost/ruote_test')
    when 'my', 'mysql'
      #Sequel.connect('mysql://root:root@localhost/ruote_test')
      Sequel.connect('mysql://root@localhost/ruote_test')
    when /:/
      Sequel.connect(ENV['RUOTE_STORAGE_DB'])
    else
      raise ArgumentError.new("unknown DB: #{ENV['RUOTE_STORAGE_DB'].inspect}")
  end

  require 'logger'

  logger = case ENV['RUOTE_STORAGE_DEBUG']
    when 'log'
      FileUtils.rm('debug.log') rescue nil
      Logger.new('debug.log')
    when 'stdout'
      Logger.new($stdout)
    else
      nil
  end

  if logger
    logger.level = Logger::DEBUG
    $sequel.loggers << logger
  end

  Ruote::Sequel.create_table($sequel, true)
    # true forces re_create of 'documents' table
end


def new_storage(opts)

  Ruote::Sequel::Storage.new($sequel, opts)
end

