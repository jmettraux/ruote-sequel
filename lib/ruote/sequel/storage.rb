#--
# Copyright (c) 2005-2011, John Mettraux, jmettraux@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
# Made in Japan.
#++

require 'sequel'
require 'ruote/storage/base'
require 'ruote/sequel/version'


module Ruote
module Sequel

  # Creates the 'documents' table necessary for this storage.
  #
  # If re_create is set to true, it will destroy any previous 'documents'
  # table and create it.
  #
  def self.create_table(sequel, re_create=false, table_name=:documents)

    m = re_create ? :create_table! : :create_table

    sequel.send(m, table_name.to_sym) do
      String :ide, :size => 255, :null => false
      Integer :rev, :null => false
      String :typ, :size => 55, :null => false
      String :doc, :text => true, :null => false
      String :wfid, :size => 255, :index => true
      String :participant_name, :size => 512
      primary_key [ :ide, :rev, :typ ]
    end
  end

  #
  # A Sequel storage implementation for ruote >= 2.2.0.
  #
  #   require 'rubygems'
  #   require 'json' # gem install json
  #   require 'ruote'
  #   require 'ruote-sequel' # gem install ruote-sequel
  #
  #   sequel = Sequel.connect('postgres://localhost/ruote_test')
  #   #sequel = Sequel.connect('mysql://root:root@localhost/ruote_test')
  #
  #   opts = { 'remote_definition_allowed' => true }
  #
  #   engine = Ruote::Engine.new(
  #     Ruote::Worker.new(
  #       Ruote::Sequel::Storage.new(sequel, opts)))
  #
  #   # ...
  #
  class Storage

    include Ruote::StorageBase

    # The underlying Sequel::Database instance
    #
    attr_reader :sequel

    def initialize(sequel, options={})

      @sequel = sequel
      @options = options
      @table = (options['sequel_table_name'] || :documents).to_sym

      put_configuration
    end

    def put_msg(action, options)

      # put_msg is a unique action, no need for all the complexity of put

      do_insert(prepare_msg_doc(action, options), 1)

      nil
    end

    def put_schedule(flavour, owner_fei, s, msg)

      # put_schedule is a unique action, no need for all the complexity of put

      doc = prepare_schedule_doc(flavour, owner_fei, s, msg)

      return nil unless doc

      do_insert(doc, 1)

      doc['_id']
    end

    def put(doc, opts={})

      if doc['_rev']

        d = get(doc['type'], doc['_id'])

        return true unless d
        return d if d['_rev'] != doc['_rev']
          # failures
      end

      nrev = doc['_rev'].to_i + 1

      begin

        do_insert(doc, nrev)

      rescue ::Sequel::DatabaseError => de

        return (get(doc['type'], doc['_id']) || true)
          # failure
      end

      @sequel[@table].where(
        :typ => doc['type'], :ide => doc['_id']
      ).filter { rev < nrev }.delete

      doc['_rev'] = nrev if opts[:update_rev]

      nil
        # success
    end

    def get(type, key)

      d = do_get(type, key)

      d ? Rufus::Json.decode(d[:doc]) : nil
    end

    def delete(doc)

      raise ArgumentError.new('no _rev for doc') unless doc['_rev']

      count = do_delete(doc)

      return (get(doc['type'], doc['_id']) || true) if count != 1
        # failure

      nil
        # success
    end

    def get_many(type, key=nil, opts={})

      ds = @sequel[@table].where(:typ => type)

      keys = key ? Array(key) : nil
      ds = ds.filter(:wfid => keys) if keys && keys.first.is_a?(String)

      return ds.all.size if opts[:count]

      ds = ds.order(
        *(opts[:descending] ? [ :ide.desc, :rev.desc ] : [ :ide.asc, :rev.asc ])
      )

      ds = ds.limit(opts[:limit], opts[:skip])

      docs = ds.all
      docs = select_last_revs(docs, opts[:descending])
      docs = docs.collect { |d| Rufus::Json.decode(d[:doc]) }

      keys && keys.first.is_a?(Regexp) ?
        docs.select { |doc| keys.find { |key| key.match(doc['_id']) } } :
        docs

      # (pass on the dataset.filter(:wfid => /regexp/) for now
      # since we have potentially multiple keys)
    end

    # Returns all the ids of the documents of a given type.
    #
    def ids(type)

      @sequel[@table].where(:typ => type).collect { |d| d[:ide] }.uniq.sort
    end

    # Nukes all the documents in this storage.
    #
    def purge!

      @sequel[@table].delete
    end

    # Returns a string representation the current content of the storage for
    # a given type.
    #
    def dump(type)

      "=== #{type} ===\n" +
      get_many(type).map { |h| "  #{h['_id']} => #{h.inspect}" }.join("\n")
    end

    # Calls #disconnect on the db. According to Sequel's doc, it closes
    # all the idle connections in the pool (not the active ones).
    #
    def shutdown

      @sequel.disconnect
    end

    # Grrr... I should sort the mess between close and shutdown...
    # Tests vs production :-(
    #
    def close

      @sequel.disconnect
    end

    # Mainly used by ruote's test/unit/ut_17_storage.rb
    #
    def add_type(type)

      # does nothing, types are differentiated by the 'typ' column
    end

    # Nukes a db type and reputs it (losing all the documents that were in it).
    #
    def purge_type!(type)

      @sequel[@table].where(:typ => type).delete
    end

    # A provision made for workitems, allow to query them directly by
    # participant name.
    #
    def by_participant(type, participant_name, opts)

      raise NotImplementedError if type != 'workitems'

      docs = @sequel[@table].where(
        :typ => type, :participant_name => participant_name)

      select_last_revs(docs).collect { |d| Rufus::Json.decode(d[:doc]) }
    end

    # Querying workitems by field (warning, goes deep into the JSON structure)
    #
    def by_field(type, field, value=nil)

      raise NotImplementedError if type != 'workitems'

      lk = [ '%"', field, '":' ]
      lk.push(Rufus::Json.encode(value)) if value
      lk.push('%')

      docs = @sequel[@table].where(:typ => type).filter(:doc.like(lk.join))

      select_last_revs(docs).collect { |d| Rufus::Json.decode(d[:doc]) }
    end

    def query_workitems(criteria)

      ds = @sequel[@table].where(:typ => 'workitems')

      return select_last_revs(ds.all).size if criteria['count']

      limit = criteria.delete('limit')
      offset = criteria.delete('offset') || criteria.delete('skip')

      ds = ds.limit(limit, offset)

      wfid =
        criteria.delete('wfid')
      pname =
        criteria.delete('participant_name') || criteria.delete('participant')

      ds = ds.filter(:ide.like("%!#{wfid}")) if wfid
      ds = ds.filter(:participant_name => pname) if pname

      criteria.collect do |k, v|
        ds = ds.filter(:doc.like("%\"#{k}\":#{Rufus::Json.encode(v)}%"))
      end

      select_last_revs(ds.all).collect { |d|
        Ruote::Workitem.new(Rufus::Json.decode(d[:doc]))
      }
    end

    protected

    def do_delete(doc)

      @sequel[@table].where(
        :ide => doc['_id'], :typ => doc['type'], :rev => doc['_rev'].to_i
      ).delete
    end

    def do_insert(doc, rev)

      @sequel[@table].insert(
        :ide => doc['_id'],
        :rev => rev,
        :typ => doc['type'],
        :doc => Rufus::Json.encode(doc.merge(
          '_rev' => rev,
          'put_at' => Ruote.now_to_utc_s)),
        :wfid => extract_wfid(doc),
        :participant_name => doc['participant_name']
      )
    end

    def extract_wfid(doc)

      doc['wfid'] || (doc['fei'] ? doc['fei']['wfid'] : nil)
    end

    def do_get(type, key)

      @sequel[@table].where(
        :typ => type, :ide => key
      ).reverse_order(:rev).first
    end

    # Don't put configuration if it's already in
    #
    # (avoid storages from trashing configuration...)
    #
    def put_configuration

      return if get('configurations', 'engine')

      conf = { '_id' => 'engine', 'type' => 'configurations' }.merge(@options)
      put(conf)
    end

    def select_last_revs(docs, reverse=false)

      docs = docs.inject({}) { |h, doc|
        h[doc[:ide]] = doc
        h
      }.values.sort_by { |h|
        h[:ide]
      }

      reverse ? docs.reverse : docs
    end
  end
end
end

