
#
# testing ruote-sequel
#
# Thu Feb 10 11:14:56 JST 2011
#

require 'yajl' rescue require 'json'
require 'rufus-json'
Rufus::Json.detect_backend

require 'ruote-sequel'

$sequel = Sequel.connect('postgres://localhost/ruote_test')
#sequel = Sequel.connect('mysql://root:root@localhost/ruote_test')

Ruote::Sequel.create_table!($sequel)

def new_storage (opts)

  Ruote::Sequel::Storage.new($sequel, opts)
end

