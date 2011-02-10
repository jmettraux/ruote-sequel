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


  #property :ide, String, :key => true, :length => 255, :required => true
  #property :rev, Integer, :key => true, :required => true
  #property :typ, String, :key => true, :required => true
  #property :doc, Text, :length => 2**32 - 1, :required => true, :lazy => false

  #property :wfid, String, :index => true
  #property :participant_name, String, :length => 512

  def self.create_table(sequel, opts={})

    m = opts[:re_create] ? :create_table! : :create_table

    sequel.send(m, :documents) do
      String :ide, :size => 255, :null => false
      String :rev, :null => false
      String :typ, :size => 55, :null => false
      String :doc, :text => true, :null => false
      String :wfid, :size => 255, :index => true
      String :participant_name, :size => 512 # INDEX !
      primary_key [ :ide, :rev, :typ ]
    end
  end

  #
  # TODO
  #
  class Storage

    include Ruote::StorageBase

    attr_reader :sequel

    def initialize(sequel, options={})

      @sequel = sequel
      @options = options

      put_configuration
    end

    def put_msg(action, options)

      # put_msg is a unique action, no need for all the complexity of put

      do_insert(prepare_msg_doc(action, options), '1')

      nil
    end

    def put_schedule(flavour, owner_fei, s, msg)

      # put_schedule is a unique action, no need for all the complexity of put

      doc = prepare_schedule_doc(flavour, owner_fei, s, msg)

      return nil unless doc

      do_insert(doc, '1')

      doc['_id']
    end

    def put(doc, opts={})

      rev = doc['_rev']

      if rev

        count = do_delete(doc)

        return (get(doc['type'], doc['_id']) || true) if count != 1
          # failure
      end

      nrev = (rev.to_i + 1).to_s

      begin

        do_insert(doc, nrev)

      rescue ::Sequel::DatabaseError => de

        return (get(doc['type'], doc['_id']) || true)
          # failure
      end

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
        #success
    end

    def get_many(type, key=nil, opts={})

      ds = @sequel[:documents].where(:typ => type)

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
    end

    # Returns all the ids of the documents of a given type.
    #
    def ids(type)

      @sequel[:documents].where(:typ => type).collect { |d| d[:ide] }.uniq.sort
    end

    # Nukes all the documents in this storage.
    #
    def purge!

      @sequel[:documents].delete_sql
    end

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

    # Mainly used by ruote's test/unit/ut_17_storage.rb
    #
    def add_type(type)

      # does nothing, types are differentiated by the 'typ' column
    end

    # Nukes a db type and reputs it (losing all the documents that were in it).
    #
    def purge_type!(type)

      @sequel[:documents].where(:typ => type).delete
    end

    # A provision made for workitems, allow to query them directly by
    # participant name.
    #
    def by_participant(type, participant_name, opts)

      raise NotImplementedError if type != 'workitems'

      query = {
        :typ => type, :participant_name => participant_name
      }.merge(opts)

      select_last_revs(Document.all(query)).collect { |d| d.to_h }
    end

    # Querying workitems by field (warning, goes deep into the JSON structure)
    #
    def by_field(type, field, value=nil)

      raise NotImplementedError if type != 'workitems'

      like = [ '%"', field, '":' ]
      like.push(Rufus::Json.encode(value)) if value
      like.push('%')

      select_last_revs(
        Document.all(:typ => type, :doc.like => like.join)
      ).collect { |d| d.to_h }
    end

    def query_workitems(criteria)

      cr = { :typ => 'workitems' }

      return select_last_revs(Document.all(cr)).size if criteria['count']

      offset = criteria.delete('offset')
      limit = criteria.delete('limit')

      wfid =
        criteria.delete('wfid')
      pname =
        criteria.delete('participant_name') || criteria.delete('participant')

      cr[:ide.like] = "%!#{wfid}" if wfid
      cr[:offset] = offset if offset
      cr[:limit] = limit if limit
      cr[:participant_name] = pname if pname

      likes = criteria.collect do |k, v|
        "%\"#{k}\":#{Rufus::Json.encode(v)}%"
      end
      cr[:conditions] = [
        ([ 'doc LIKE ?' ] * likes.size).join(' AND '), *likes
      ] unless likes.empty?

      select_last_revs(
        Document.all(cr)
      ).collect { |d| Ruote::Workitem.new(d.to_h) }
    end

    protected

    def do_delete(doc)

      @sequel[:documents].where(
        :ide => doc['_id'], :typ => doc['type'], :rev => doc['_rev']
      ).delete
    end

    def do_insert(doc, rev)

      @sequel[:documents].insert(
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

      @sequel[:documents].where(
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

