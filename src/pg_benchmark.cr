require "json"
require "benchmark"
require "placeos-models"
require "auto_initialize"
require "clear"
require "digest/md5"

TENANT_ID = (ENV["TENANT_ID"]? || "2").to_i
PG_DATABASE_URL         = ENV["PG_DATABASE_URL"]
PG_CONNECTION_POOL_SIZE = ENV["PG_CONNECTION_POOL_SIZE"]?.presence.try(&.to_i?) || 50

Clear::SQL.init(PG_DATABASE_URL)
PgORM::Database.parse(PG_DATABASE_URL)

# Clear version of ORM
class Booking
  struct History
    include JSON::Serializable

    property state : Booking::State
    property time : Int64
    property source : String?

    def initialize(@state : Booking::State, @time : Int64, @source : String? = nil)
    end

    class Converter
      def self.to_column(x) : Array(History)?
        case x
        when Nil
          nil
        when JSON::PullParser
          Array(History).from_json x.read_raw
        when JSON::Any
          Array(History).from_json x.to_json
        when Array(History)
          x
        else
          raise "Cannot convert from #{x.class} to Array(Booking::History)"
        end
      end

      def self.to_db(x : Array(History)?)
        x.to_json
      end
    end
  end
end

class Booking
  include Clear::Model

  struct BookingResponse
    include JSON::Serializable
    include AutoInitialize

    getter id : Int64
    getter booking_type : String
    getter booking_start : Int64
    getter booking_end : Int64
    getter timezone : String?
    getter asset_id : String
    getter user_id : String
    @[JSON::Field(format: "email")]
    getter user_email : String
    getter user_name : String
    getter zones : Array(String)?
    getter process_state : String?
    getter last_changed : Int64?
    getter approved : Bool
    getter approved_at : Int64?
    getter rejected : Bool
    getter rejected_at : Int64?
    getter approver_id : String?
    getter approver_name : String?
    @[JSON::Field(format: "email")]
    getter approver_email : String?
    getter department : String?
    @[JSON::Field(description: "provided if this booking is associated with a calendar event")]
    getter event_id : String?
    getter title : String?
    getter checked_in : Bool
    getter checked_in_at : Int64?
    getter checked_out_at : Int64?
    getter description : String?
    getter deleted : Bool
    getter deleted_at : Int64?
    @[JSON::Field(format: "email")]
    getter booked_by_email : String
    getter booked_by_name : String
    getter booked_from : String?
    getter extension_data : JSON::Any
    getter current_state : State
    getter history : Array(History)
  end

  enum State
    Reserved   # Booking starts in the future, no one has checked-in and it hasn't been deleted
    CheckedIn  # Booking is currently active (the wall clock time is between start and end times of the booking) and the user has checked in
    CheckedOut # The user checked out during the start and end times
    NoShow     # It's past the end time of the booking and it was never checked in
    Rejected   # Someone rejected the booking before it started
    Cancelled  # The booking was deleted before the booking start time
    Ended      # The current time is past the end of the booking, the user checked-in but never checked-out
    Unknown
  end

  column id : Int64, primary: true, presence: false

  column user_id : String
  column user_email : String
  column user_name : String
  column asset_id : String
  column zones : Array(String)? # default in migration

  column email_digest : String?

  column booking_type : String
  column booking_start : Int64
  column booking_end : Int64
  column timezone : String?

  column title : String?
  column description : String?

  column deleted : Bool, presence: false
  column deleted_at : Int64?

  column checked_in : Bool, presence: false
  column checked_in_at : Int64?
  column checked_out_at : Int64?

  column rejected : Bool, presence: false
  column rejected_at : Int64?
  column approved : Bool, presence: false
  column approved_at : Int64?
  column approver_id : String?
  column approver_email : String?
  column approver_name : String?
  column department : String?
  column event_id : String?

  column booked_by_id : String
  column booked_by_email : String

  column booked_by_email_digest : String?
  column booked_by_name : String

  # if we want to record the system that performed the bookings
  # (kiosk, mobile, swipe etc)
  column booked_from : String?

  # used to hold information relating to the state of the booking process
  column process_state : String?
  column last_changed : Int64?
  column created : Int64?

  column extension_data : JSON::Any, presence: false
  column history : Array(History), presence: false

  property utm_source : String? = nil

  # belongs_to tenant : Tenant
  # has_many attendees : Attendee, foreign_key: "booking_id"

  before :create, :set_created

  def validate
    validate_booking_time
  end

  before(:save) do |m|
    booking_model = m.as(Booking)
    booking_model.user_id = booking_model.booked_by_id if !booking_model.user_id_column.defined?
    booking_model.user_email = booking_model.booked_by_email if !booking_model.user_email_column.defined?
    booking_model.user_name = booking_model.booked_by_name if !booking_model.user_name_column.defined?
    booking_model.approver_email = booking_model.approver_email if booking_model.approver_email_column.defined?
    booking_model.email_digest = Digest::MD5.hexdigest booking_model.user_email
    booking_model.booked_by_email_digest = Digest::MD5.hexdigest booking_model.booked_by_email
    booking_model.booked_from = booking_model.utm_source if !booking_model.booked_from_column.defined?
    booking_model.history = booking_model.current_history
    Log.error { {
      message: "History contains more than 3 events.",
      id:      booking_model.id,
    } } if booking_model.history.size > 3
  end

  def current_history : Array(History)
    state = current_state
    history_column.value([] of History).dup.tap do |booking_history|
      if booking_history.empty? || booking_history.last.state != state
        booking_history << History.new(state, Time.local.to_unix, @utm_source) unless state.unknown?
      end
    end
  end

  def set_created
    self.last_changed = self.created = Time.utc.to_unix
  end

  private def validate_booking_time
    add_error("booking_end", "must be after booking_start") if booking_end <= booking_start
  end

  scope :by_tenant do |tenant_id|
    where { var("bookings", "tenant_id") == tenant_id }
  end

  scope :by_user_id do |user_id|
    user_id ? where(user_id: user_id) : self
  end

  scope :by_user_or_email do |user_id_value, user_email_value, include_booked_by|
    # TODO: Construct `user_or_email` query correctly
    booked_by = include_booked_by ? %( OR "booked_by_id" = '#{user_id_value}') : ""
    user_id_value = user_id_value.try &.gsub(/[\'\"\)\(\\\/\$\?\;\:\<\>\+\=\*\&\^\#\!\`\%\}\{\[\]]/, "")
    user_email_value = user_email_value.try &.gsub(/[\'\"\)\(\\\/\$\?\;\:\<\>\=\*\&\^\!\`\%\}\{\[\]]/, "")

    user_email_digest = Digest::MD5.hexdigest(user_email_value.to_s) if user_email_value

    if user_id_value && user_email_digest
      where(%(("user_id" = '#{user_id_value}' OR "email_digest" = '#{user_email_digest}'#{booked_by})))
    elsif user_id_value
      where(%(("user_id" = '#{user_id_value}'#{booked_by})))
    elsif user_email_digest
      booked_by = include_booked_by ? %( OR "booked_by_email_digest" = '#{user_email_digest}') : ""
      where(%(("email_digest" = '#{user_email_digest}'#{booked_by})))
    else
      self
    end
  end

  scope :is_extension_data do |value|
    if value
      parse = value.delete &.in?('{', '}')
      array = parse.split(",")
      array.each do |entry|
        split_entry = entry.split(":")
        where { extension_data.jsonb(split_entry[0]) == split_entry[1] }
      end
    else
      self
    end
  end

  scope :is_state do |state|
    state ? where(process_state: state) : self
  end

  scope :is_created_before do |time|
    time ? where { last_changed < time.not_nil!.to_i64 } : self
  end

  scope :is_created_after do |time|
    time ? where { last_changed > time.not_nil!.to_i64 } : self
  end

  scope :is_event_id do |event_id|
    event_id ? where(event_id: event_id) : self
  end

  scope :booked_between do |tenant_id, period_start, period_end|
    by_tenant(tenant_id)
      .inner_join("attendees") { var("bookings", "id") == var("attendees", "booking_id") }
      .where("bookings.booking_start >= :period_start AND bookings.booking_end <= :period_end", period_start: period_start, period_end: period_end)
  end

  TRUTHY = {true, "true"}

  scope :is_approved do |value|
    if !value.nil?
      check = value.in?(TRUTHY)
      where { approved == check }
    else
      self
    end
  end

  scope :is_rejected do |value|
    if !value.nil?
      check = value.in?(TRUTHY)
      where { rejected == check }
    else
      self
    end
  end

  scope :is_checked_in do |value|
    if !value.nil?
      check = value.in?(TRUTHY)
      where { checked_in == check }
    else
      self
    end
  end

  scope :is_department do |value|
    if value
      where { department == value }
    else
      self
    end
  end

  # Bookings have the zones in an array.
  #
  # In case of multiple zones as input,
  # we return all bookings that have
  # any of the input zones in their zones array
  scope :by_zones do |zones|
    return self if zones.empty?

    # https://www.postgresql.org/docs/9.1/arrays.html#ARRAYS-SEARCHING
    query = zones.join(" OR ") do |zone|
      zone = zone.gsub(/[\'\"\)\(\\\/\$\?\;\:\<\>\.\+\=\*\&\^\#\!\`\%\}\{\[\]]/, "")
      "( '#{zone}' = ANY (zones) )"
    end

    where("( #{query} )")
  end

  # Booking ends in the future, no one has checked-in and it hasn't been deleted
  protected def is_reserved?(current_time : Int64 = Time.local.to_unix)
    booking_end > current_time &&
      !checked_in_column.value(nil) &&
      !checked_in_at_column.value(nil) &&
      !deleted_at_column.value(nil) &&
      !rejected_at_column.value(nil) &&
      !checked_out_at_column.value(nil)
  end

  # Booking ends in the future, the user has checked in and it is not cancelled
  protected def is_checked_in?(current_time : Int64 = Time.local.to_unix)
    checked_in_at_column.value(nil) &&
      checked_in_column.value(nil) &&
      !checked_out_at_column.value(nil) &&
      booking_end > current_time &&
      !is_cancelled?
  end

  # The user checked out before the end time
  protected def is_checked_out?
    (co_at = checked_out_at_column.value(nil)) &&
      booking_end >= co_at
  end

  # It's past the end time of the booking and it was never checked in
  # or the booking was deleted between the start and end time and it was never checked in
  protected def is_no_show?(current_time : Int64 = Time.local.to_unix)
    !checked_in_at_column.value(nil) &&
      !is_cancelled? &&
      (booking_end < current_time ||
        ((del_at = deleted_at_column.value(nil)) &&
          booking_end >= del_at))
  end

  # Someone rejected the booking before it started
  protected def is_rejected?
    (r_at = rejected_at_column.value(nil)) &&
      booking_start > r_at
  end

  # The booking was deleted before the booking start time
  # or before the booking end time if checked in
  protected def is_cancelled?
    (del_at = deleted_at_column.value(nil)) &&
      (booking_start > del_at ||
        (booking_end > del_at && checked_in))
  end

  # The current time is past the end of the booking, the user checked-in but never checked-out
  protected def is_ended?(current_time : Int64 = Time.local.to_unix)
    !checked_out_at_column.value(nil) &&
      checked_in_at_column.value(nil) &&
      booking_end < current_time
  end

  def current_state : State
    current_time = Time.local.to_unix

    case self
    when .is_reserved?(current_time)   then State::Reserved
    when .is_checked_in?(current_time) then State::CheckedIn
    when .is_checked_out?              then State::CheckedOut
    when .is_no_show?(current_time)    then State::NoShow
    when .is_rejected?                 then State::Rejected
    when .is_cancelled?                then State::Cancelled
    when .is_ended?                    then State::Ended
    else
      Log.error { {
        message:        "Booking is in an Unknown state.",
        id:             id_column.value(nil),
        current_time:   current_time,
        booking_start:  booking_start,
        booking_end:    booking_end,
        rejected_at:    rejected_at_column.value(nil),
        checked_in_at:  checked_in_at_column.value(nil),
        checked_out_at: checked_out_at_column.value(nil),
        deleted_at:     deleted_at_column.value(nil),
      } }
      State::Unknown
    end
  end

  def as_h : BookingResponse
    BookingResponse.new(
      id: id,
      booking_type: booking_type,
      booking_start: booking_start,
      booking_end: booking_end,
      timezone: timezone,
      asset_id: asset_id,
      user_id: user_id,
      user_email: user_email.to_s,
      user_name: user_name,
      zones: zones,
      process_state: process_state,
      last_changed: last_changed,
      approved: approved,
      approved_at: approved_at,
      rejected: rejected,
      rejected_at: rejected_at,
      approver_id: approver_id,
      approver_name: approver_name,
      approver_email: approver_email,
      department: department,
      event_id: event_id,
      title: title,
      checked_in: checked_in,
      checked_in_at: checked_in_at,
      checked_out_at: checked_out_at,
      description: description,
      deleted: deleted,
      deleted_at: deleted_at,
      booked_by_email: booked_by_email.to_s,
      booked_by_name: booked_by_name,
      booked_from: booked_from,
      extension_data: extension_data,
      current_state: current_state,
      history: history,
    )
  end
end

Clear::Model::Converter.add_converter("Array(Booking::History)", Booking::History::Converter)

module PgBenchmark
  VERSION = "0.1.0"

  alias PgBooking = PlaceOS::Model::Booking

  def self.pg_make_query(ending : Int64, starting : Int64, booking_type : String, zones : String)
    query = PgBooking.by_tenant(TENANT_ID).where(
      %("booking_start" < ? AND "booking_end" > ? AND "booking_type" = ?),
      ending, starting, booking_type
    )
    zones = Set.new((zones || "").split(',').map(&.strip).reject(&.empty?)).to_a
    query = query.by_zones(zones) unless zones.empty?
    query = query
      .order(booking_start: :desc)
      .where(deleted: false)
      .limit(20000)
    query.to_a.to_json
  end

  def self.clear_make_query(ending : Int64, starting : Int64, booking_type : String, zones : String)
    query = Booking.query.by_tenant(TENANT_ID).where(
      %("booking_start" < :ending AND "booking_end" > :starting AND "booking_type" = :booking_type),
      starting: starting, ending: ending, booking_type: booking_type)
    zones = Set.new((zones || "").split(',').map(&.strip).reject(&.empty?)).to_a
    query = query.by_zones(zones) unless zones.empty?
    query = query
      .order_by(:booking_start, :desc)
      .where(deleted: false)
      .limit(20000)
    result = query.to_a.map &.as_h
    result.to_json
  end
end

require "option_parser"

ending = 1685520000
starting = 1685502271
booking_type = "desk"
zones = "zone-CfeL8ROfjK,zone-CfeL9JjWxZ"

json_data = nil
Benchmark.ips do |bm|
  bm.report("PG ORM") do
    json_data = PgBenchmark.pg_make_query(ending, starting, booking_type, zones)
  end

  bm.report("Clear ORM") do
    json_data = PgBenchmark.clear_make_query(ending, starting, booking_type, zones)
  end
end
