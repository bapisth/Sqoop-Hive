// ORM class for table 'mdm_incident_hi_detail'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Mon Oct 16 13:22:00 IST 2017
// For connector: org.apache.sqoop.manager.PostgresqlManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class mdm_incident_hi_detail extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  protected ResultSet __cur_result_set;
  private String proc_inst_id;
  public String get_proc_inst_id() {
    return proc_inst_id;
  }
  public void set_proc_inst_id(String proc_inst_id) {
    this.proc_inst_id = proc_inst_id;
  }
  public mdm_incident_hi_detail with_proc_inst_id(String proc_inst_id) {
    this.proc_inst_id = proc_inst_id;
    return this;
  }
  private String status;
  public String get_status() {
    return status;
  }
  public void set_status(String status) {
    this.status = status;
  }
  public mdm_incident_hi_detail with_status(String status) {
    this.status = status;
    return this;
  }
  private String assigned_to;
  public String get_assigned_to() {
    return assigned_to;
  }
  public void set_assigned_to(String assigned_to) {
    this.assigned_to = assigned_to;
  }
  public mdm_incident_hi_detail with_assigned_to(String assigned_to) {
    this.assigned_to = assigned_to;
    return this;
  }
  private String assigned_to_type;
  public String get_assigned_to_type() {
    return assigned_to_type;
  }
  public void set_assigned_to_type(String assigned_to_type) {
    this.assigned_to_type = assigned_to_type;
  }
  public mdm_incident_hi_detail with_assigned_to_type(String assigned_to_type) {
    this.assigned_to_type = assigned_to_type;
    return this;
  }
  private java.sql.Timestamp updated_time_stamp;
  public java.sql.Timestamp get_updated_time_stamp() {
    return updated_time_stamp;
  }
  public void set_updated_time_stamp(java.sql.Timestamp updated_time_stamp) {
    this.updated_time_stamp = updated_time_stamp;
  }
  public mdm_incident_hi_detail with_updated_time_stamp(java.sql.Timestamp updated_time_stamp) {
    this.updated_time_stamp = updated_time_stamp;
    return this;
  }
  private String updated_by;
  public String get_updated_by() {
    return updated_by;
  }
  public void set_updated_by(String updated_by) {
    this.updated_by = updated_by;
  }
  public mdm_incident_hi_detail with_updated_by(String updated_by) {
    this.updated_by = updated_by;
    return this;
  }
  private String priority;
  public String get_priority() {
    return priority;
  }
  public void set_priority(String priority) {
    this.priority = priority;
  }
  public mdm_incident_hi_detail with_priority(String priority) {
    this.priority = priority;
    return this;
  }
  private String severity;
  public String get_severity() {
    return severity;
  }
  public void set_severity(String severity) {
    this.severity = severity;
  }
  public mdm_incident_hi_detail with_severity(String severity) {
    this.severity = severity;
    return this;
  }
  private String category;
  public String get_category() {
    return category;
  }
  public void set_category(String category) {
    this.category = category;
  }
  public mdm_incident_hi_detail with_category(String category) {
    this.category = category;
    return this;
  }
  private String sub_category;
  public String get_sub_category() {
    return sub_category;
  }
  public void set_sub_category(String sub_category) {
    this.sub_category = sub_category;
  }
  public mdm_incident_hi_detail with_sub_category(String sub_category) {
    this.sub_category = sub_category;
    return this;
  }
  private String tenant_id_;
  public String get_tenant_id_() {
    return tenant_id_;
  }
  public void set_tenant_id_(String tenant_id_) {
    this.tenant_id_ = tenant_id_;
  }
  public mdm_incident_hi_detail with_tenant_id_(String tenant_id_) {
    this.tenant_id_ = tenant_id_;
    return this;
  }
  private Long id;
  public Long get_id() {
    return id;
  }
  public void set_id(Long id) {
    this.id = id;
  }
  public mdm_incident_hi_detail with_id(Long id) {
    this.id = id;
    return this;
  }
  private String title;
  public String get_title() {
    return title;
  }
  public void set_title(String title) {
    this.title = title;
  }
  public mdm_incident_hi_detail with_title(String title) {
    this.title = title;
    return this;
  }
  private java.sql.Timestamp incident_occured_date;
  public java.sql.Timestamp get_incident_occured_date() {
    return incident_occured_date;
  }
  public void set_incident_occured_date(java.sql.Timestamp incident_occured_date) {
    this.incident_occured_date = incident_occured_date;
  }
  public mdm_incident_hi_detail with_incident_occured_date(java.sql.Timestamp incident_occured_date) {
    this.incident_occured_date = incident_occured_date;
    return this;
  }
  private String bus_no;
  public String get_bus_no() {
    return bus_no;
  }
  public void set_bus_no(String bus_no) {
    this.bus_no = bus_no;
  }
  public mdm_incident_hi_detail with_bus_no(String bus_no) {
    this.bus_no = bus_no;
    return this;
  }
  private String operator_id;
  public String get_operator_id() {
    return operator_id;
  }
  public void set_operator_id(String operator_id) {
    this.operator_id = operator_id;
  }
  public mdm_incident_hi_detail with_operator_id(String operator_id) {
    this.operator_id = operator_id;
    return this;
  }
  private String equipment_id;
  public String get_equipment_id() {
    return equipment_id;
  }
  public void set_equipment_id(String equipment_id) {
    this.equipment_id = equipment_id;
  }
  public mdm_incident_hi_detail with_equipment_id(String equipment_id) {
    this.equipment_id = equipment_id;
    return this;
  }
  private String equipment_name;
  public String get_equipment_name() {
    return equipment_name;
  }
  public void set_equipment_name(String equipment_name) {
    this.equipment_name = equipment_name;
  }
  public mdm_incident_hi_detail with_equipment_name(String equipment_name) {
    this.equipment_name = equipment_name;
    return this;
  }
  private String route;
  public String get_route() {
    return route;
  }
  public void set_route(String route) {
    this.route = route;
  }
  public mdm_incident_hi_detail with_route(String route) {
    this.route = route;
    return this;
  }
  private String stop;
  public String get_stop() {
    return stop;
  }
  public void set_stop(String stop) {
    this.stop = stop;
  }
  public mdm_incident_hi_detail with_stop(String stop) {
    this.stop = stop;
    return this;
  }
  private String location;
  public String get_location() {
    return location;
  }
  public void set_location(String location) {
    this.location = location;
  }
  public mdm_incident_hi_detail with_location(String location) {
    this.location = location;
    return this;
  }
  private String incident_description;
  public String get_incident_description() {
    return incident_description;
  }
  public void set_incident_description(String incident_description) {
    this.incident_description = incident_description;
  }
  public mdm_incident_hi_detail with_incident_description(String incident_description) {
    this.incident_description = incident_description;
    return this;
  }
  private String resolution;
  public String get_resolution() {
    return resolution;
  }
  public void set_resolution(String resolution) {
    this.resolution = resolution;
  }
  public mdm_incident_hi_detail with_resolution(String resolution) {
    this.resolution = resolution;
    return this;
  }
  private String reference_id;
  public String get_reference_id() {
    return reference_id;
  }
  public void set_reference_id(String reference_id) {
    this.reference_id = reference_id;
  }
  public mdm_incident_hi_detail with_reference_id(String reference_id) {
    this.reference_id = reference_id;
    return this;
  }
  private String process_definition;
  public String get_process_definition() {
    return process_definition;
  }
  public void set_process_definition(String process_definition) {
    this.process_definition = process_definition;
  }
  public mdm_incident_hi_detail with_process_definition(String process_definition) {
    this.process_definition = process_definition;
    return this;
  }
  private Long fine_amount;
  public Long get_fine_amount() {
    return fine_amount;
  }
  public void set_fine_amount(Long fine_amount) {
    this.fine_amount = fine_amount;
  }
  public mdm_incident_hi_detail with_fine_amount(Long fine_amount) {
    this.fine_amount = fine_amount;
    return this;
  }
  private String fine_unit;
  public String get_fine_unit() {
    return fine_unit;
  }
  public void set_fine_unit(String fine_unit) {
    this.fine_unit = fine_unit;
  }
  public mdm_incident_hi_detail with_fine_unit(String fine_unit) {
    this.fine_unit = fine_unit;
    return this;
  }
  private String created_by;
  public String get_created_by() {
    return created_by;
  }
  public void set_created_by(String created_by) {
    this.created_by = created_by;
  }
  public mdm_incident_hi_detail with_created_by(String created_by) {
    this.created_by = created_by;
    return this;
  }
  private String station_code;
  public String get_station_code() {
    return station_code;
  }
  public void set_station_code(String station_code) {
    this.station_code = station_code;
  }
  public mdm_incident_hi_detail with_station_code(String station_code) {
    this.station_code = station_code;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof mdm_incident_hi_detail)) {
      return false;
    }
    mdm_incident_hi_detail that = (mdm_incident_hi_detail) o;
    boolean equal = true;
    equal = equal && (this.proc_inst_id == null ? that.proc_inst_id == null : this.proc_inst_id.equals(that.proc_inst_id));
    equal = equal && (this.status == null ? that.status == null : this.status.equals(that.status));
    equal = equal && (this.assigned_to == null ? that.assigned_to == null : this.assigned_to.equals(that.assigned_to));
    equal = equal && (this.assigned_to_type == null ? that.assigned_to_type == null : this.assigned_to_type.equals(that.assigned_to_type));
    equal = equal && (this.updated_time_stamp == null ? that.updated_time_stamp == null : this.updated_time_stamp.equals(that.updated_time_stamp));
    equal = equal && (this.updated_by == null ? that.updated_by == null : this.updated_by.equals(that.updated_by));
    equal = equal && (this.priority == null ? that.priority == null : this.priority.equals(that.priority));
    equal = equal && (this.severity == null ? that.severity == null : this.severity.equals(that.severity));
    equal = equal && (this.category == null ? that.category == null : this.category.equals(that.category));
    equal = equal && (this.sub_category == null ? that.sub_category == null : this.sub_category.equals(that.sub_category));
    equal = equal && (this.tenant_id_ == null ? that.tenant_id_ == null : this.tenant_id_.equals(that.tenant_id_));
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.title == null ? that.title == null : this.title.equals(that.title));
    equal = equal && (this.incident_occured_date == null ? that.incident_occured_date == null : this.incident_occured_date.equals(that.incident_occured_date));
    equal = equal && (this.bus_no == null ? that.bus_no == null : this.bus_no.equals(that.bus_no));
    equal = equal && (this.operator_id == null ? that.operator_id == null : this.operator_id.equals(that.operator_id));
    equal = equal && (this.equipment_id == null ? that.equipment_id == null : this.equipment_id.equals(that.equipment_id));
    equal = equal && (this.equipment_name == null ? that.equipment_name == null : this.equipment_name.equals(that.equipment_name));
    equal = equal && (this.route == null ? that.route == null : this.route.equals(that.route));
    equal = equal && (this.stop == null ? that.stop == null : this.stop.equals(that.stop));
    equal = equal && (this.location == null ? that.location == null : this.location.equals(that.location));
    equal = equal && (this.incident_description == null ? that.incident_description == null : this.incident_description.equals(that.incident_description));
    equal = equal && (this.resolution == null ? that.resolution == null : this.resolution.equals(that.resolution));
    equal = equal && (this.reference_id == null ? that.reference_id == null : this.reference_id.equals(that.reference_id));
    equal = equal && (this.process_definition == null ? that.process_definition == null : this.process_definition.equals(that.process_definition));
    equal = equal && (this.fine_amount == null ? that.fine_amount == null : this.fine_amount.equals(that.fine_amount));
    equal = equal && (this.fine_unit == null ? that.fine_unit == null : this.fine_unit.equals(that.fine_unit));
    equal = equal && (this.created_by == null ? that.created_by == null : this.created_by.equals(that.created_by));
    equal = equal && (this.station_code == null ? that.station_code == null : this.station_code.equals(that.station_code));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof mdm_incident_hi_detail)) {
      return false;
    }
    mdm_incident_hi_detail that = (mdm_incident_hi_detail) o;
    boolean equal = true;
    equal = equal && (this.proc_inst_id == null ? that.proc_inst_id == null : this.proc_inst_id.equals(that.proc_inst_id));
    equal = equal && (this.status == null ? that.status == null : this.status.equals(that.status));
    equal = equal && (this.assigned_to == null ? that.assigned_to == null : this.assigned_to.equals(that.assigned_to));
    equal = equal && (this.assigned_to_type == null ? that.assigned_to_type == null : this.assigned_to_type.equals(that.assigned_to_type));
    equal = equal && (this.updated_time_stamp == null ? that.updated_time_stamp == null : this.updated_time_stamp.equals(that.updated_time_stamp));
    equal = equal && (this.updated_by == null ? that.updated_by == null : this.updated_by.equals(that.updated_by));
    equal = equal && (this.priority == null ? that.priority == null : this.priority.equals(that.priority));
    equal = equal && (this.severity == null ? that.severity == null : this.severity.equals(that.severity));
    equal = equal && (this.category == null ? that.category == null : this.category.equals(that.category));
    equal = equal && (this.sub_category == null ? that.sub_category == null : this.sub_category.equals(that.sub_category));
    equal = equal && (this.tenant_id_ == null ? that.tenant_id_ == null : this.tenant_id_.equals(that.tenant_id_));
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.title == null ? that.title == null : this.title.equals(that.title));
    equal = equal && (this.incident_occured_date == null ? that.incident_occured_date == null : this.incident_occured_date.equals(that.incident_occured_date));
    equal = equal && (this.bus_no == null ? that.bus_no == null : this.bus_no.equals(that.bus_no));
    equal = equal && (this.operator_id == null ? that.operator_id == null : this.operator_id.equals(that.operator_id));
    equal = equal && (this.equipment_id == null ? that.equipment_id == null : this.equipment_id.equals(that.equipment_id));
    equal = equal && (this.equipment_name == null ? that.equipment_name == null : this.equipment_name.equals(that.equipment_name));
    equal = equal && (this.route == null ? that.route == null : this.route.equals(that.route));
    equal = equal && (this.stop == null ? that.stop == null : this.stop.equals(that.stop));
    equal = equal && (this.location == null ? that.location == null : this.location.equals(that.location));
    equal = equal && (this.incident_description == null ? that.incident_description == null : this.incident_description.equals(that.incident_description));
    equal = equal && (this.resolution == null ? that.resolution == null : this.resolution.equals(that.resolution));
    equal = equal && (this.reference_id == null ? that.reference_id == null : this.reference_id.equals(that.reference_id));
    equal = equal && (this.process_definition == null ? that.process_definition == null : this.process_definition.equals(that.process_definition));
    equal = equal && (this.fine_amount == null ? that.fine_amount == null : this.fine_amount.equals(that.fine_amount));
    equal = equal && (this.fine_unit == null ? that.fine_unit == null : this.fine_unit.equals(that.fine_unit));
    equal = equal && (this.created_by == null ? that.created_by == null : this.created_by.equals(that.created_by));
    equal = equal && (this.station_code == null ? that.station_code == null : this.station_code.equals(that.station_code));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.proc_inst_id = JdbcWritableBridge.readString(1, __dbResults);
    this.status = JdbcWritableBridge.readString(2, __dbResults);
    this.assigned_to = JdbcWritableBridge.readString(3, __dbResults);
    this.assigned_to_type = JdbcWritableBridge.readString(4, __dbResults);
    this.updated_time_stamp = JdbcWritableBridge.readTimestamp(5, __dbResults);
    this.updated_by = JdbcWritableBridge.readString(6, __dbResults);
    this.priority = JdbcWritableBridge.readString(7, __dbResults);
    this.severity = JdbcWritableBridge.readString(8, __dbResults);
    this.category = JdbcWritableBridge.readString(9, __dbResults);
    this.sub_category = JdbcWritableBridge.readString(10, __dbResults);
    this.tenant_id_ = JdbcWritableBridge.readString(11, __dbResults);
    this.id = JdbcWritableBridge.readLong(12, __dbResults);
    this.title = JdbcWritableBridge.readString(13, __dbResults);
    this.incident_occured_date = JdbcWritableBridge.readTimestamp(14, __dbResults);
    this.bus_no = JdbcWritableBridge.readString(15, __dbResults);
    this.operator_id = JdbcWritableBridge.readString(16, __dbResults);
    this.equipment_id = JdbcWritableBridge.readString(17, __dbResults);
    this.equipment_name = JdbcWritableBridge.readString(18, __dbResults);
    this.route = JdbcWritableBridge.readString(19, __dbResults);
    this.stop = JdbcWritableBridge.readString(20, __dbResults);
    this.location = JdbcWritableBridge.readString(21, __dbResults);
    this.incident_description = JdbcWritableBridge.readString(22, __dbResults);
    this.resolution = JdbcWritableBridge.readString(23, __dbResults);
    this.reference_id = JdbcWritableBridge.readString(24, __dbResults);
    this.process_definition = JdbcWritableBridge.readString(25, __dbResults);
    this.fine_amount = JdbcWritableBridge.readLong(26, __dbResults);
    this.fine_unit = JdbcWritableBridge.readString(27, __dbResults);
    this.created_by = JdbcWritableBridge.readString(28, __dbResults);
    this.station_code = JdbcWritableBridge.readString(29, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.proc_inst_id = JdbcWritableBridge.readString(1, __dbResults);
    this.status = JdbcWritableBridge.readString(2, __dbResults);
    this.assigned_to = JdbcWritableBridge.readString(3, __dbResults);
    this.assigned_to_type = JdbcWritableBridge.readString(4, __dbResults);
    this.updated_time_stamp = JdbcWritableBridge.readTimestamp(5, __dbResults);
    this.updated_by = JdbcWritableBridge.readString(6, __dbResults);
    this.priority = JdbcWritableBridge.readString(7, __dbResults);
    this.severity = JdbcWritableBridge.readString(8, __dbResults);
    this.category = JdbcWritableBridge.readString(9, __dbResults);
    this.sub_category = JdbcWritableBridge.readString(10, __dbResults);
    this.tenant_id_ = JdbcWritableBridge.readString(11, __dbResults);
    this.id = JdbcWritableBridge.readLong(12, __dbResults);
    this.title = JdbcWritableBridge.readString(13, __dbResults);
    this.incident_occured_date = JdbcWritableBridge.readTimestamp(14, __dbResults);
    this.bus_no = JdbcWritableBridge.readString(15, __dbResults);
    this.operator_id = JdbcWritableBridge.readString(16, __dbResults);
    this.equipment_id = JdbcWritableBridge.readString(17, __dbResults);
    this.equipment_name = JdbcWritableBridge.readString(18, __dbResults);
    this.route = JdbcWritableBridge.readString(19, __dbResults);
    this.stop = JdbcWritableBridge.readString(20, __dbResults);
    this.location = JdbcWritableBridge.readString(21, __dbResults);
    this.incident_description = JdbcWritableBridge.readString(22, __dbResults);
    this.resolution = JdbcWritableBridge.readString(23, __dbResults);
    this.reference_id = JdbcWritableBridge.readString(24, __dbResults);
    this.process_definition = JdbcWritableBridge.readString(25, __dbResults);
    this.fine_amount = JdbcWritableBridge.readLong(26, __dbResults);
    this.fine_unit = JdbcWritableBridge.readString(27, __dbResults);
    this.created_by = JdbcWritableBridge.readString(28, __dbResults);
    this.station_code = JdbcWritableBridge.readString(29, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(proc_inst_id, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(status, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(assigned_to, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(assigned_to_type, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(updated_time_stamp, 5 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(updated_by, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(priority, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(severity, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(category, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(sub_category, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(tenant_id_, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeLong(id, 12 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(title, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(incident_occured_date, 14 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(bus_no, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(operator_id, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(equipment_id, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(equipment_name, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(route, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(stop, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(location, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(incident_description, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(resolution, 23 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(reference_id, 24 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(process_definition, 25 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeLong(fine_amount, 26 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(fine_unit, 27 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(created_by, 28 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(station_code, 29 + __off, 12, __dbStmt);
    return 29;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(proc_inst_id, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(status, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(assigned_to, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(assigned_to_type, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(updated_time_stamp, 5 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(updated_by, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(priority, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(severity, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(category, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(sub_category, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(tenant_id_, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeLong(id, 12 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(title, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(incident_occured_date, 14 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(bus_no, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(operator_id, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(equipment_id, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(equipment_name, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(route, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(stop, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(location, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(incident_description, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(resolution, 23 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(reference_id, 24 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(process_definition, 25 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeLong(fine_amount, 26 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(fine_unit, 27 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(created_by, 28 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(station_code, 29 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.proc_inst_id = null;
    } else {
    this.proc_inst_id = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.status = null;
    } else {
    this.status = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.assigned_to = null;
    } else {
    this.assigned_to = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.assigned_to_type = null;
    } else {
    this.assigned_to_type = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.updated_time_stamp = null;
    } else {
    this.updated_time_stamp = new Timestamp(__dataIn.readLong());
    this.updated_time_stamp.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.updated_by = null;
    } else {
    this.updated_by = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.priority = null;
    } else {
    this.priority = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.severity = null;
    } else {
    this.severity = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.category = null;
    } else {
    this.category = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.sub_category = null;
    } else {
    this.sub_category = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.tenant_id_ = null;
    } else {
    this.tenant_id_ = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.id = null;
    } else {
    this.id = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.title = null;
    } else {
    this.title = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.incident_occured_date = null;
    } else {
    this.incident_occured_date = new Timestamp(__dataIn.readLong());
    this.incident_occured_date.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.bus_no = null;
    } else {
    this.bus_no = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.operator_id = null;
    } else {
    this.operator_id = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.equipment_id = null;
    } else {
    this.equipment_id = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.equipment_name = null;
    } else {
    this.equipment_name = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.route = null;
    } else {
    this.route = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.stop = null;
    } else {
    this.stop = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.location = null;
    } else {
    this.location = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.incident_description = null;
    } else {
    this.incident_description = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.resolution = null;
    } else {
    this.resolution = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.reference_id = null;
    } else {
    this.reference_id = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.process_definition = null;
    } else {
    this.process_definition = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.fine_amount = null;
    } else {
    this.fine_amount = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.fine_unit = null;
    } else {
    this.fine_unit = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.created_by = null;
    } else {
    this.created_by = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.station_code = null;
    } else {
    this.station_code = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.proc_inst_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, proc_inst_id);
    }
    if (null == this.status) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, status);
    }
    if (null == this.assigned_to) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, assigned_to);
    }
    if (null == this.assigned_to_type) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, assigned_to_type);
    }
    if (null == this.updated_time_stamp) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.updated_time_stamp.getTime());
    __dataOut.writeInt(this.updated_time_stamp.getNanos());
    }
    if (null == this.updated_by) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, updated_by);
    }
    if (null == this.priority) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, priority);
    }
    if (null == this.severity) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, severity);
    }
    if (null == this.category) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, category);
    }
    if (null == this.sub_category) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, sub_category);
    }
    if (null == this.tenant_id_) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, tenant_id_);
    }
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.id);
    }
    if (null == this.title) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, title);
    }
    if (null == this.incident_occured_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.incident_occured_date.getTime());
    __dataOut.writeInt(this.incident_occured_date.getNanos());
    }
    if (null == this.bus_no) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, bus_no);
    }
    if (null == this.operator_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, operator_id);
    }
    if (null == this.equipment_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, equipment_id);
    }
    if (null == this.equipment_name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, equipment_name);
    }
    if (null == this.route) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, route);
    }
    if (null == this.stop) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, stop);
    }
    if (null == this.location) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, location);
    }
    if (null == this.incident_description) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, incident_description);
    }
    if (null == this.resolution) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, resolution);
    }
    if (null == this.reference_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, reference_id);
    }
    if (null == this.process_definition) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, process_definition);
    }
    if (null == this.fine_amount) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.fine_amount);
    }
    if (null == this.fine_unit) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, fine_unit);
    }
    if (null == this.created_by) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, created_by);
    }
    if (null == this.station_code) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, station_code);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.proc_inst_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, proc_inst_id);
    }
    if (null == this.status) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, status);
    }
    if (null == this.assigned_to) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, assigned_to);
    }
    if (null == this.assigned_to_type) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, assigned_to_type);
    }
    if (null == this.updated_time_stamp) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.updated_time_stamp.getTime());
    __dataOut.writeInt(this.updated_time_stamp.getNanos());
    }
    if (null == this.updated_by) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, updated_by);
    }
    if (null == this.priority) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, priority);
    }
    if (null == this.severity) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, severity);
    }
    if (null == this.category) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, category);
    }
    if (null == this.sub_category) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, sub_category);
    }
    if (null == this.tenant_id_) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, tenant_id_);
    }
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.id);
    }
    if (null == this.title) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, title);
    }
    if (null == this.incident_occured_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.incident_occured_date.getTime());
    __dataOut.writeInt(this.incident_occured_date.getNanos());
    }
    if (null == this.bus_no) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, bus_no);
    }
    if (null == this.operator_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, operator_id);
    }
    if (null == this.equipment_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, equipment_id);
    }
    if (null == this.equipment_name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, equipment_name);
    }
    if (null == this.route) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, route);
    }
    if (null == this.stop) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, stop);
    }
    if (null == this.location) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, location);
    }
    if (null == this.incident_description) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, incident_description);
    }
    if (null == this.resolution) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, resolution);
    }
    if (null == this.reference_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, reference_id);
    }
    if (null == this.process_definition) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, process_definition);
    }
    if (null == this.fine_amount) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.fine_amount);
    }
    if (null == this.fine_unit) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, fine_unit);
    }
    if (null == this.created_by) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, created_by);
    }
    if (null == this.station_code) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, station_code);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(proc_inst_id==null?"null":proc_inst_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(status==null?"null":status, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(assigned_to==null?"null":assigned_to, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(assigned_to_type==null?"null":assigned_to_type, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(updated_time_stamp==null?"null":"" + updated_time_stamp, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(updated_by==null?"null":updated_by, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(priority==null?"null":priority, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(severity==null?"null":severity, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(category==null?"null":category, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(sub_category==null?"null":sub_category, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(tenant_id_==null?"null":tenant_id_, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"null":"" + id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(title==null?"null":title, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(incident_occured_date==null?"null":"" + incident_occured_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(bus_no==null?"null":bus_no, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(operator_id==null?"null":operator_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(equipment_id==null?"null":equipment_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(equipment_name==null?"null":equipment_name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(route==null?"null":route, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(stop==null?"null":stop, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(location==null?"null":location, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(incident_description==null?"null":incident_description, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(resolution==null?"null":resolution, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(reference_id==null?"null":reference_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(process_definition==null?"null":process_definition, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(fine_amount==null?"null":"" + fine_amount, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(fine_unit==null?"null":fine_unit, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(created_by==null?"null":created_by, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(station_code==null?"null":station_code, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(proc_inst_id==null?"null":proc_inst_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(status==null?"null":status, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(assigned_to==null?"null":assigned_to, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(assigned_to_type==null?"null":assigned_to_type, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(updated_time_stamp==null?"null":"" + updated_time_stamp, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(updated_by==null?"null":updated_by, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(priority==null?"null":priority, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(severity==null?"null":severity, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(category==null?"null":category, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(sub_category==null?"null":sub_category, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(tenant_id_==null?"null":tenant_id_, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"null":"" + id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(title==null?"null":title, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(incident_occured_date==null?"null":"" + incident_occured_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(bus_no==null?"null":bus_no, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(operator_id==null?"null":operator_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(equipment_id==null?"null":equipment_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(equipment_name==null?"null":equipment_name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(route==null?"null":route, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(stop==null?"null":stop, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(location==null?"null":location, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(incident_description==null?"null":incident_description, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(resolution==null?"null":resolution, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(reference_id==null?"null":reference_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(process_definition==null?"null":process_definition, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(fine_amount==null?"null":"" + fine_amount, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(fine_unit==null?"null":fine_unit, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(created_by==null?"null":created_by, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(station_code==null?"null":station_code, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.proc_inst_id = null; } else {
      this.proc_inst_id = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.status = null; } else {
      this.status = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.assigned_to = null; } else {
      this.assigned_to = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.assigned_to_type = null; } else {
      this.assigned_to_type = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.updated_time_stamp = null; } else {
      this.updated_time_stamp = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.updated_by = null; } else {
      this.updated_by = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.priority = null; } else {
      this.priority = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.severity = null; } else {
      this.severity = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.category = null; } else {
      this.category = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.sub_category = null; } else {
      this.sub_category = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.tenant_id_ = null; } else {
      this.tenant_id_ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.id = null; } else {
      this.id = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.title = null; } else {
      this.title = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.incident_occured_date = null; } else {
      this.incident_occured_date = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.bus_no = null; } else {
      this.bus_no = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.operator_id = null; } else {
      this.operator_id = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.equipment_id = null; } else {
      this.equipment_id = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.equipment_name = null; } else {
      this.equipment_name = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.route = null; } else {
      this.route = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.stop = null; } else {
      this.stop = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.location = null; } else {
      this.location = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.incident_description = null; } else {
      this.incident_description = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.resolution = null; } else {
      this.resolution = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.reference_id = null; } else {
      this.reference_id = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.process_definition = null; } else {
      this.process_definition = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.fine_amount = null; } else {
      this.fine_amount = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.fine_unit = null; } else {
      this.fine_unit = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.created_by = null; } else {
      this.created_by = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.station_code = null; } else {
      this.station_code = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.proc_inst_id = null; } else {
      this.proc_inst_id = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.status = null; } else {
      this.status = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.assigned_to = null; } else {
      this.assigned_to = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.assigned_to_type = null; } else {
      this.assigned_to_type = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.updated_time_stamp = null; } else {
      this.updated_time_stamp = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.updated_by = null; } else {
      this.updated_by = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.priority = null; } else {
      this.priority = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.severity = null; } else {
      this.severity = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.category = null; } else {
      this.category = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.sub_category = null; } else {
      this.sub_category = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.tenant_id_ = null; } else {
      this.tenant_id_ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.id = null; } else {
      this.id = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.title = null; } else {
      this.title = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.incident_occured_date = null; } else {
      this.incident_occured_date = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.bus_no = null; } else {
      this.bus_no = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.operator_id = null; } else {
      this.operator_id = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.equipment_id = null; } else {
      this.equipment_id = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.equipment_name = null; } else {
      this.equipment_name = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.route = null; } else {
      this.route = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.stop = null; } else {
      this.stop = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.location = null; } else {
      this.location = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.incident_description = null; } else {
      this.incident_description = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.resolution = null; } else {
      this.resolution = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.reference_id = null; } else {
      this.reference_id = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.process_definition = null; } else {
      this.process_definition = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.fine_amount = null; } else {
      this.fine_amount = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.fine_unit = null; } else {
      this.fine_unit = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.created_by = null; } else {
      this.created_by = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.station_code = null; } else {
      this.station_code = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    mdm_incident_hi_detail o = (mdm_incident_hi_detail) super.clone();
    o.updated_time_stamp = (o.updated_time_stamp != null) ? (java.sql.Timestamp) o.updated_time_stamp.clone() : null;
    o.incident_occured_date = (o.incident_occured_date != null) ? (java.sql.Timestamp) o.incident_occured_date.clone() : null;
    return o;
  }

  public void clone0(mdm_incident_hi_detail o) throws CloneNotSupportedException {
    o.updated_time_stamp = (o.updated_time_stamp != null) ? (java.sql.Timestamp) o.updated_time_stamp.clone() : null;
    o.incident_occured_date = (o.incident_occured_date != null) ? (java.sql.Timestamp) o.incident_occured_date.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new TreeMap<String, Object>();
    __sqoop$field_map.put("proc_inst_id", this.proc_inst_id);
    __sqoop$field_map.put("status", this.status);
    __sqoop$field_map.put("assigned_to", this.assigned_to);
    __sqoop$field_map.put("assigned_to_type", this.assigned_to_type);
    __sqoop$field_map.put("updated_time_stamp", this.updated_time_stamp);
    __sqoop$field_map.put("updated_by", this.updated_by);
    __sqoop$field_map.put("priority", this.priority);
    __sqoop$field_map.put("severity", this.severity);
    __sqoop$field_map.put("category", this.category);
    __sqoop$field_map.put("sub_category", this.sub_category);
    __sqoop$field_map.put("tenant_id_", this.tenant_id_);
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("title", this.title);
    __sqoop$field_map.put("incident_occured_date", this.incident_occured_date);
    __sqoop$field_map.put("bus_no", this.bus_no);
    __sqoop$field_map.put("operator_id", this.operator_id);
    __sqoop$field_map.put("equipment_id", this.equipment_id);
    __sqoop$field_map.put("equipment_name", this.equipment_name);
    __sqoop$field_map.put("route", this.route);
    __sqoop$field_map.put("stop", this.stop);
    __sqoop$field_map.put("location", this.location);
    __sqoop$field_map.put("incident_description", this.incident_description);
    __sqoop$field_map.put("resolution", this.resolution);
    __sqoop$field_map.put("reference_id", this.reference_id);
    __sqoop$field_map.put("process_definition", this.process_definition);
    __sqoop$field_map.put("fine_amount", this.fine_amount);
    __sqoop$field_map.put("fine_unit", this.fine_unit);
    __sqoop$field_map.put("created_by", this.created_by);
    __sqoop$field_map.put("station_code", this.station_code);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("proc_inst_id", this.proc_inst_id);
    __sqoop$field_map.put("status", this.status);
    __sqoop$field_map.put("assigned_to", this.assigned_to);
    __sqoop$field_map.put("assigned_to_type", this.assigned_to_type);
    __sqoop$field_map.put("updated_time_stamp", this.updated_time_stamp);
    __sqoop$field_map.put("updated_by", this.updated_by);
    __sqoop$field_map.put("priority", this.priority);
    __sqoop$field_map.put("severity", this.severity);
    __sqoop$field_map.put("category", this.category);
    __sqoop$field_map.put("sub_category", this.sub_category);
    __sqoop$field_map.put("tenant_id_", this.tenant_id_);
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("title", this.title);
    __sqoop$field_map.put("incident_occured_date", this.incident_occured_date);
    __sqoop$field_map.put("bus_no", this.bus_no);
    __sqoop$field_map.put("operator_id", this.operator_id);
    __sqoop$field_map.put("equipment_id", this.equipment_id);
    __sqoop$field_map.put("equipment_name", this.equipment_name);
    __sqoop$field_map.put("route", this.route);
    __sqoop$field_map.put("stop", this.stop);
    __sqoop$field_map.put("location", this.location);
    __sqoop$field_map.put("incident_description", this.incident_description);
    __sqoop$field_map.put("resolution", this.resolution);
    __sqoop$field_map.put("reference_id", this.reference_id);
    __sqoop$field_map.put("process_definition", this.process_definition);
    __sqoop$field_map.put("fine_amount", this.fine_amount);
    __sqoop$field_map.put("fine_unit", this.fine_unit);
    __sqoop$field_map.put("created_by", this.created_by);
    __sqoop$field_map.put("station_code", this.station_code);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if ("proc_inst_id".equals(__fieldName)) {
      this.proc_inst_id = (String) __fieldVal;
    }
    else    if ("status".equals(__fieldName)) {
      this.status = (String) __fieldVal;
    }
    else    if ("assigned_to".equals(__fieldName)) {
      this.assigned_to = (String) __fieldVal;
    }
    else    if ("assigned_to_type".equals(__fieldName)) {
      this.assigned_to_type = (String) __fieldVal;
    }
    else    if ("updated_time_stamp".equals(__fieldName)) {
      this.updated_time_stamp = (java.sql.Timestamp) __fieldVal;
    }
    else    if ("updated_by".equals(__fieldName)) {
      this.updated_by = (String) __fieldVal;
    }
    else    if ("priority".equals(__fieldName)) {
      this.priority = (String) __fieldVal;
    }
    else    if ("severity".equals(__fieldName)) {
      this.severity = (String) __fieldVal;
    }
    else    if ("category".equals(__fieldName)) {
      this.category = (String) __fieldVal;
    }
    else    if ("sub_category".equals(__fieldName)) {
      this.sub_category = (String) __fieldVal;
    }
    else    if ("tenant_id_".equals(__fieldName)) {
      this.tenant_id_ = (String) __fieldVal;
    }
    else    if ("id".equals(__fieldName)) {
      this.id = (Long) __fieldVal;
    }
    else    if ("title".equals(__fieldName)) {
      this.title = (String) __fieldVal;
    }
    else    if ("incident_occured_date".equals(__fieldName)) {
      this.incident_occured_date = (java.sql.Timestamp) __fieldVal;
    }
    else    if ("bus_no".equals(__fieldName)) {
      this.bus_no = (String) __fieldVal;
    }
    else    if ("operator_id".equals(__fieldName)) {
      this.operator_id = (String) __fieldVal;
    }
    else    if ("equipment_id".equals(__fieldName)) {
      this.equipment_id = (String) __fieldVal;
    }
    else    if ("equipment_name".equals(__fieldName)) {
      this.equipment_name = (String) __fieldVal;
    }
    else    if ("route".equals(__fieldName)) {
      this.route = (String) __fieldVal;
    }
    else    if ("stop".equals(__fieldName)) {
      this.stop = (String) __fieldVal;
    }
    else    if ("location".equals(__fieldName)) {
      this.location = (String) __fieldVal;
    }
    else    if ("incident_description".equals(__fieldName)) {
      this.incident_description = (String) __fieldVal;
    }
    else    if ("resolution".equals(__fieldName)) {
      this.resolution = (String) __fieldVal;
    }
    else    if ("reference_id".equals(__fieldName)) {
      this.reference_id = (String) __fieldVal;
    }
    else    if ("process_definition".equals(__fieldName)) {
      this.process_definition = (String) __fieldVal;
    }
    else    if ("fine_amount".equals(__fieldName)) {
      this.fine_amount = (Long) __fieldVal;
    }
    else    if ("fine_unit".equals(__fieldName)) {
      this.fine_unit = (String) __fieldVal;
    }
    else    if ("created_by".equals(__fieldName)) {
      this.created_by = (String) __fieldVal;
    }
    else    if ("station_code".equals(__fieldName)) {
      this.station_code = (String) __fieldVal;
    }
    else {
      throw new RuntimeException("No such field: " + __fieldName);
    }
  }
  public boolean setField0(String __fieldName, Object __fieldVal) {
    if ("proc_inst_id".equals(__fieldName)) {
      this.proc_inst_id = (String) __fieldVal;
      return true;
    }
    else    if ("status".equals(__fieldName)) {
      this.status = (String) __fieldVal;
      return true;
    }
    else    if ("assigned_to".equals(__fieldName)) {
      this.assigned_to = (String) __fieldVal;
      return true;
    }
    else    if ("assigned_to_type".equals(__fieldName)) {
      this.assigned_to_type = (String) __fieldVal;
      return true;
    }
    else    if ("updated_time_stamp".equals(__fieldName)) {
      this.updated_time_stamp = (java.sql.Timestamp) __fieldVal;
      return true;
    }
    else    if ("updated_by".equals(__fieldName)) {
      this.updated_by = (String) __fieldVal;
      return true;
    }
    else    if ("priority".equals(__fieldName)) {
      this.priority = (String) __fieldVal;
      return true;
    }
    else    if ("severity".equals(__fieldName)) {
      this.severity = (String) __fieldVal;
      return true;
    }
    else    if ("category".equals(__fieldName)) {
      this.category = (String) __fieldVal;
      return true;
    }
    else    if ("sub_category".equals(__fieldName)) {
      this.sub_category = (String) __fieldVal;
      return true;
    }
    else    if ("tenant_id_".equals(__fieldName)) {
      this.tenant_id_ = (String) __fieldVal;
      return true;
    }
    else    if ("id".equals(__fieldName)) {
      this.id = (Long) __fieldVal;
      return true;
    }
    else    if ("title".equals(__fieldName)) {
      this.title = (String) __fieldVal;
      return true;
    }
    else    if ("incident_occured_date".equals(__fieldName)) {
      this.incident_occured_date = (java.sql.Timestamp) __fieldVal;
      return true;
    }
    else    if ("bus_no".equals(__fieldName)) {
      this.bus_no = (String) __fieldVal;
      return true;
    }
    else    if ("operator_id".equals(__fieldName)) {
      this.operator_id = (String) __fieldVal;
      return true;
    }
    else    if ("equipment_id".equals(__fieldName)) {
      this.equipment_id = (String) __fieldVal;
      return true;
    }
    else    if ("equipment_name".equals(__fieldName)) {
      this.equipment_name = (String) __fieldVal;
      return true;
    }
    else    if ("route".equals(__fieldName)) {
      this.route = (String) __fieldVal;
      return true;
    }
    else    if ("stop".equals(__fieldName)) {
      this.stop = (String) __fieldVal;
      return true;
    }
    else    if ("location".equals(__fieldName)) {
      this.location = (String) __fieldVal;
      return true;
    }
    else    if ("incident_description".equals(__fieldName)) {
      this.incident_description = (String) __fieldVal;
      return true;
    }
    else    if ("resolution".equals(__fieldName)) {
      this.resolution = (String) __fieldVal;
      return true;
    }
    else    if ("reference_id".equals(__fieldName)) {
      this.reference_id = (String) __fieldVal;
      return true;
    }
    else    if ("process_definition".equals(__fieldName)) {
      this.process_definition = (String) __fieldVal;
      return true;
    }
    else    if ("fine_amount".equals(__fieldName)) {
      this.fine_amount = (Long) __fieldVal;
      return true;
    }
    else    if ("fine_unit".equals(__fieldName)) {
      this.fine_unit = (String) __fieldVal;
      return true;
    }
    else    if ("created_by".equals(__fieldName)) {
      this.created_by = (String) __fieldVal;
      return true;
    }
    else    if ("station_code".equals(__fieldName)) {
      this.station_code = (String) __fieldVal;
      return true;
    }
    else {
      return false;    }
  }
}
