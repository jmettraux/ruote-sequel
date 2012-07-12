
#
# testing ruote-sequel
#
# Thu Feb 10 11:14:56 JST 2011
#

require 'yajl' rescue require 'json'
require 'rufus-json'
Rufus::Json.detect_backend

require 'ruote-sequel'

unless $sequel

  $sequel = Sequel.connect('postgres://localhost/ruote_test')

  #$sequel = Sequel.connect('mysql://root:root@localhost/ruote_test')
  #$sequel = Sequel.connect('mysql://root@localhost/ruote_test')

  Ruote::Sequel.create_table($sequel, true)
    # true forces re_create of 'documents' table

  require 'logger'

  logger = nil

  case ENV['RUOTE_STORAGE_DEBUG']
    when 'log'
      FileUtils.rm('debug.log') rescue nil
      logger = Logger.new('debug.log')
    when 'stdout'
      logger = Logger.new($stdout)
  end

  if logger
    logger.level = Logger::DEBUG
    $sequel.loggers << logger
  end
end


def new_storage(opts)

  Ruote::Sequel::Storage.new($sequel, opts)
end

