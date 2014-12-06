#--
# Copyright (c) 2005-2013, John Mettraux, jmettraux@gmail.com
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
  # table and create it. If false (default) then the table will be created
  # if it doesn't already exist.
  #
  # It's also possible to change the default table_name from 'documents' to
  # something else with the optional third parameter
  #
  def self.create_table(sequel, re_create=false, table_name='documents')

    m = re_create ? :create_table! : :create_table?

    sequel.send(m, table_name.to_sym) do

      String :ide, :size => 255, :null => false
      Integer :rev, :null => false
      String :typ, :size => 55, :null => false
      String :doc, :text => true, :null => false
      String :wfid, :size => 255
      String :participant_name, :size => 512
      String :owner
      DateTime :due_at
      String :task, :size => 20

      primary_key [ :typ, :ide, :rev ]

      index [ :typ, :wfid, :owner, :due_at, :task ]
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
  # At initialization, this storage will create the table in the database
  # if missing.
  #
  class Storage

    include Ruote::StorageBase

    # The underlying Sequel::Database instance
    #
    attr_reader :sequel

    # Creates the Sequel-based storage.
    #
    # Will create the table it needs if it is not yet present.
    #
    def initialize(sequel, options={})

      @sequel = sequel
      @table = (options['sequel_table_name'] || :documents).to_sym

      Ruote::Sequel.create_table(@sequel, false, @table)

      replace_engine_configuration(options)
    end

    def put_msg(action, options)

      # put_msg is a unique action, no need for all the complexity of put

      do_insert(prepare_msg_doc(action, options), 1)

      nil
    end

    # Used to reserve 'msgs' and 'schedules'. Simply deletes the document,
    # return true if the delete was successful (ie if the reservation is
    # valid).
    #
    def reserve(doc)

      @sequel[@table].where(
        :typ => doc['type'], :ide => doc['_id'], :rev => 1
      ).delete > 0
    end

    def put_schedule(flavour, owner_fei, s, msg)

      # put_schedule is a unique action, no need for all the complexity of put

      doc = prepare_schedule_doc(flavour, owner_fei, s, msg)

      return nil unless doc

      do_insert(doc, 1)

      doc['_id']
    end

    def put(doc, opts={})

      cache_clear(doc)

      if doc['_rev']

        d = get(doc['type'], doc['_id'])

        return true unless d
        return d if d['_rev'] != doc['_rev']
          # failures
      end

      nrev = doc['_rev'].to_i + 1

      begin

        do_insert(doc, nrev, opts[:update_rev])

      rescue ::Sequel::DatabaseError => de

        return (get(doc['type'], doc['_id']) || true)
          # failure
      end

      @sequel[@table].where(
        :typ => doc['type'], :ide => doc['_id']
      ).filter { rev < nrev }.delete

      nil
        # success
    end

    def get(type, key)

      cache_get(type, key) || do_get(type, key)
    end

    def delete(doc)

      raise ArgumentError.new('no _rev for doc') unless doc['_rev']

      cache_clear(doc)
        # usually not necessary, adding it not to forget it later on

      count = @sequel[@table].where(
        :typ => doc['type'], :ide => doc['_id'], :rev => doc['_rev'].to_i
      ).delete

      return (get(doc['type'], doc['_id']) || true) if count < 1
        # failure

      nil
        # success
    end

    def get_many(type, key=nil, opts={})

      cached = cache_get_many(type, key, opts)
      return cached if cached

      ds = @sequel[@table].where(:typ => type)

      keys = key ? Array(key) : nil
      ds = ds.filter(:wfid => keys) if keys && keys.first.is_a?(String)

      return ds.count if opts[:count]

      ds = ds.order(
        opts[:descending] ? ::Sequel.desc(:ide) : ::Sequel.asc(:ide), ::Sequel.desc(:rev)
      ).limit(
        opts[:limit], opts[:skip] || opts[:offset]
      )

      docs = select_last_revs(ds)
      docs = docs.collect { |d| decode_doc(d) }

      if keys && keys.first.is_a?(Regexp)
        docs.select { |doc| keys.find { |key| key.match(doc['_id']) } }
      else
        docs
      end

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
    def by_participant(type, participant_name, opts={})

      raise NotImplementedError if type != 'workitems'

      docs = @sequel[@table].where(
        :typ => type, :participant_name => participant_name
      )

      return docs.count if opts[:count]

      docs = docs.order(
        ::Sequel.asc(:ide), ::Sequel.desc(:rev)
      ).limit(
        opts[:limit], opts[:offset] || opts[:skip]
      )

      select_last_revs(docs).collect { |d| Ruote::Workitem.from_json(d[:doc]) }
    end

    # Querying workitems by field (warning, goes deep into the JSON structure)
    #
    def by_field(type, field, value, opts={})

      raise NotImplementedError if type != 'workitems'

      lk = [ '%"', field, '":' ]
      lk.push(Rufus::Json.encode(value)) if value
      lk.push('%')

      docs = @sequel[@table].where(
        :typ => type
      ).filter(
        ::Sequel.like(:doc, lk.join)
      )

      return docs.count if opts[:count]

      docs = docs.order(
        ::Sequel.asc(:ide), ::Sequel.desc(:rev)
      ).limit(
        opts[:limit], opts[:offset] || opts[:skip]
      )

      select_last_revs(docs).collect { |d| Ruote::Workitem.from_json(d[:doc]) }
    end

    def query_workitems(criteria)

      ds = @sequel[@table].where(:typ => 'workitems')

      count = criteria.delete('count')

      limit = criteria.delete('limit')
      offset = criteria.delete('offset') || criteria.delete('skip')

      wfid =
        criteria.delete('wfid')
      pname =
        criteria.delete('participant_name') || criteria.delete('participant')

      ds = ds.filter(::Sequel.like(:ide, "%!#{wfid}")) if wfid
      ds = ds.filter(:participant_name => pname) if pname

      criteria.collect do |k, v|
        ds = ds.filter(::Sequel.like(:doc, "%\"#{k}\":#{Rufus::Json.encode(v)}%"))
      end

      return ds.count if count

      ds = ds.order(::Sequel.asc(:ide), ::Sequel.desc(:rev)).limit(limit, offset)

      select_last_revs(ds).collect { |d| Ruote::Workitem.from_json(d[:doc]) }
    end

    # Used by the worker to indicate a new step begins. For ruote-sequel,
    # it means the cache can be prepared (a unique select yielding
    # all the info necessary for one worker step (expressions excluded)).
    #
    def begin_step

      prepare_cache
    end

    protected

    def decode_doc(doc)

      return nil if doc.nil?

      doc = doc[:doc]
      doc = doc.read if doc.respond_to?(:read)

      Rufus::Json.decode(doc)
    end

    def do_insert(doc, rev, update_rev=false)

      doc = doc.send(
        update_rev ? :merge! : :merge,
        { '_rev' => rev, 'put_at' => Ruote.now_to_utc_s })

      # Use bound variables
      # http://sequel.rubyforge.org/rdoc/files/doc/prepared_statements_rdoc.html
      #
      # That makes Oracle happy (the doc field might > 4000 characters)
      #
      # Thanks Geoff Herney
      #
      @sequel[@table].call(
        :insert, {
          :ide => (doc['_id'] || ''),
          :rev => (rev || ''),
          :typ => (doc['type'] || ''),
          :doc => (Rufus::Json.encode(doc) || ''),
          :wfid => (extract_wfid(doc) || ''),
          :participant_name => (doc['participant_name'] || ''),
          :owner => doc['owner'],
          :due_at => extract_due_at(doc),
          :task => extract_task_name(doc)
        }, {
          :ide => :$ide,
          :rev => :$rev,
          :typ => :$typ,
          :doc => :$doc,
          :wfid => :$wfid,
          :participant_name => :$participant_name,
          :owner => :$owner,
          :due_at => :$due_at,
          :task => :$task
        })
    end

    def try_get(doc, *paths)
      paths.inject(doc) do |val, path|
        val[path] if val.is_a? Hash
      end
    end

    def extract_due_at(doc)

      try_get(doc, 'fields', 'due_at')
    end

    def extract_task_name(doc)

      try_get(doc, 'fields', 'params', 'task')
    end

    def extract_wfid(doc)

      doc['wfid'] || (doc['fei'] ? doc['fei']['wfid'] : nil)
    end

    def do_get(type, key)

      d = @sequel[@table].select(:doc).where(
        :typ => type, :ide => key
      ).reverse_order(:rev).first

      decode_doc(d)
    end

    # Weed out older docs (same ide, smaller rev).
    #
    # This could all have been done via SQL, but those inconsistencies
    # are rare, the cost of the pumped SQL is not constant :-(
    #
    def select_last_revs(docs)

      docs.each_with_object([]) { |doc, a|
        a << doc if a.last.nil? || doc[:ide] != a.last[:ide]
      }
    end

    #--
    # worker step cache
    #
    # in order to cut down the number of selects, do one select with
    # all the information the worker needs for one step of work
    #++

    CACHED_TYPES = %w[ msgs schedules configurations variables ]

    # One select to grab in all the info necessary for a worker step
    # (expressions excepted).
    #
    def prepare_cache

      CACHED_TYPES.each { |t| cache[t] = {} }

      @sequel[@table].select(
        :ide, :typ, :doc
      ).where(
        :typ => CACHED_TYPES
      ).order(
        ::Sequel.asc(:ide), ::Sequel.desc(:rev)
      ).each do |d|
        (cache[d[:typ]] ||= {})[d[:ide]] ||= decode_doc(d)
      end

      cache['variables']['trackers'] ||=
        { '_id' => 'trackers', 'type' => 'variables', 'trackers' => {} }
    end

    # Ask the cache for a doc. Returns nil if it's not cached.
    #
    def cache_get(type, key)

      (cache[type] || {})[key]
    end

    # Ask the cache for a set of documents. Returns nil if it's not cached
    # or caching is not OK.
    #
    def cache_get_many(type, keys, options)

      if !options[:batch] && CACHED_TYPES.include?(type) && cache[type]
        cache[type].values
      else
        nil
      end
    end

    # Removes a document from the cache.
    #
    def cache_clear(doc)

      (cache[doc['type']] || {}).delete(doc['_id'])
    end

    # Returns the cache for the given thread. Returns {} if there is no
    # cache available.
    #
    def cache

      worker = Thread.current['ruote_worker']

      return {} unless worker

      (Thread.current["cache_#{worker.name}"] ||= {})
    end
  end
end
end
