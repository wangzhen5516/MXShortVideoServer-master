/**
 * Autogenerated by Thrift Compiler (0.11.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package mx.j2.recommend.thrift;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.11.0)", date = "2021-07-08")
public class ExtraClientInfo implements org.apache.thrift.TBase<ExtraClientInfo, ExtraClientInfo._Fields>, java.io.Serializable, Cloneable, Comparable<ExtraClientInfo> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ExtraClientInfo");

  private static final org.apache.thrift.protocol.TField LAST_INTERACTIVE_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("lastInteractiveId", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField LAST_INTERACTIVE_TIMESTAMP_FIELD_DESC = new org.apache.thrift.protocol.TField("lastInteractiveTimestamp", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField LAST_INTERACTIVE_TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("lastInteractiveType", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField IS_SUPPORT_BATCH_DATA_FIELD_DESC = new org.apache.thrift.protocol.TField("isSupportBatchData", org.apache.thrift.protocol.TType.BOOL, (short)4);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new ExtraClientInfoStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new ExtraClientInfoTupleSchemeFactory();

  public java.lang.String lastInteractiveId; // optional
  public java.lang.String lastInteractiveTimestamp; // optional
  public java.lang.String lastInteractiveType; // optional
  public boolean isSupportBatchData; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    LAST_INTERACTIVE_ID((short)1, "lastInteractiveId"),
    LAST_INTERACTIVE_TIMESTAMP((short)2, "lastInteractiveTimestamp"),
    LAST_INTERACTIVE_TYPE((short)3, "lastInteractiveType"),
    IS_SUPPORT_BATCH_DATA((short)4, "isSupportBatchData");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // LAST_INTERACTIVE_ID
          return LAST_INTERACTIVE_ID;
        case 2: // LAST_INTERACTIVE_TIMESTAMP
          return LAST_INTERACTIVE_TIMESTAMP;
        case 3: // LAST_INTERACTIVE_TYPE
          return LAST_INTERACTIVE_TYPE;
        case 4: // IS_SUPPORT_BATCH_DATA
          return IS_SUPPORT_BATCH_DATA;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __ISSUPPORTBATCHDATA_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.LAST_INTERACTIVE_ID,_Fields.LAST_INTERACTIVE_TIMESTAMP,_Fields.LAST_INTERACTIVE_TYPE,_Fields.IS_SUPPORT_BATCH_DATA};
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.LAST_INTERACTIVE_ID, new org.apache.thrift.meta_data.FieldMetaData("lastInteractiveId", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.LAST_INTERACTIVE_TIMESTAMP, new org.apache.thrift.meta_data.FieldMetaData("lastInteractiveTimestamp", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.LAST_INTERACTIVE_TYPE, new org.apache.thrift.meta_data.FieldMetaData("lastInteractiveType", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.IS_SUPPORT_BATCH_DATA, new org.apache.thrift.meta_data.FieldMetaData("isSupportBatchData", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ExtraClientInfo.class, metaDataMap);
  }

  public ExtraClientInfo() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ExtraClientInfo(ExtraClientInfo other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetLastInteractiveId()) {
      this.lastInteractiveId = other.lastInteractiveId;
    }
    if (other.isSetLastInteractiveTimestamp()) {
      this.lastInteractiveTimestamp = other.lastInteractiveTimestamp;
    }
    if (other.isSetLastInteractiveType()) {
      this.lastInteractiveType = other.lastInteractiveType;
    }
    this.isSupportBatchData = other.isSupportBatchData;
  }

  public ExtraClientInfo deepCopy() {
    return new ExtraClientInfo(this);
  }

  @Override
  public void clear() {
    this.lastInteractiveId = null;
    this.lastInteractiveTimestamp = null;
    this.lastInteractiveType = null;
    setIsSupportBatchDataIsSet(false);
    this.isSupportBatchData = false;
  }

  public java.lang.String getLastInteractiveId() {
    return this.lastInteractiveId;
  }

  public ExtraClientInfo setLastInteractiveId(java.lang.String lastInteractiveId) {
    this.lastInteractiveId = lastInteractiveId;
    return this;
  }

  public void unsetLastInteractiveId() {
    this.lastInteractiveId = null;
  }

  /** Returns true if field lastInteractiveId is set (has been assigned a value) and false otherwise */
  public boolean isSetLastInteractiveId() {
    return this.lastInteractiveId != null;
  }

  public void setLastInteractiveIdIsSet(boolean value) {
    if (!value) {
      this.lastInteractiveId = null;
    }
  }

  public java.lang.String getLastInteractiveTimestamp() {
    return this.lastInteractiveTimestamp;
  }

  public ExtraClientInfo setLastInteractiveTimestamp(java.lang.String lastInteractiveTimestamp) {
    this.lastInteractiveTimestamp = lastInteractiveTimestamp;
    return this;
  }

  public void unsetLastInteractiveTimestamp() {
    this.lastInteractiveTimestamp = null;
  }

  /** Returns true if field lastInteractiveTimestamp is set (has been assigned a value) and false otherwise */
  public boolean isSetLastInteractiveTimestamp() {
    return this.lastInteractiveTimestamp != null;
  }

  public void setLastInteractiveTimestampIsSet(boolean value) {
    if (!value) {
      this.lastInteractiveTimestamp = null;
    }
  }

  public java.lang.String getLastInteractiveType() {
    return this.lastInteractiveType;
  }

  public ExtraClientInfo setLastInteractiveType(java.lang.String lastInteractiveType) {
    this.lastInteractiveType = lastInteractiveType;
    return this;
  }

  public void unsetLastInteractiveType() {
    this.lastInteractiveType = null;
  }

  /** Returns true if field lastInteractiveType is set (has been assigned a value) and false otherwise */
  public boolean isSetLastInteractiveType() {
    return this.lastInteractiveType != null;
  }

  public void setLastInteractiveTypeIsSet(boolean value) {
    if (!value) {
      this.lastInteractiveType = null;
    }
  }

  public boolean isIsSupportBatchData() {
    return this.isSupportBatchData;
  }

  public ExtraClientInfo setIsSupportBatchData(boolean isSupportBatchData) {
    this.isSupportBatchData = isSupportBatchData;
    setIsSupportBatchDataIsSet(true);
    return this;
  }

  public void unsetIsSupportBatchData() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __ISSUPPORTBATCHDATA_ISSET_ID);
  }

  /** Returns true if field isSupportBatchData is set (has been assigned a value) and false otherwise */
  public boolean isSetIsSupportBatchData() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __ISSUPPORTBATCHDATA_ISSET_ID);
  }

  public void setIsSupportBatchDataIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __ISSUPPORTBATCHDATA_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, java.lang.Object value) {
    switch (field) {
    case LAST_INTERACTIVE_ID:
      if (value == null) {
        unsetLastInteractiveId();
      } else {
        setLastInteractiveId((java.lang.String)value);
      }
      break;

    case LAST_INTERACTIVE_TIMESTAMP:
      if (value == null) {
        unsetLastInteractiveTimestamp();
      } else {
        setLastInteractiveTimestamp((java.lang.String)value);
      }
      break;

    case LAST_INTERACTIVE_TYPE:
      if (value == null) {
        unsetLastInteractiveType();
      } else {
        setLastInteractiveType((java.lang.String)value);
      }
      break;

    case IS_SUPPORT_BATCH_DATA:
      if (value == null) {
        unsetIsSupportBatchData();
      } else {
        setIsSupportBatchData((java.lang.Boolean)value);
      }
      break;

    }
  }

  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case LAST_INTERACTIVE_ID:
      return getLastInteractiveId();

    case LAST_INTERACTIVE_TIMESTAMP:
      return getLastInteractiveTimestamp();

    case LAST_INTERACTIVE_TYPE:
      return getLastInteractiveType();

    case IS_SUPPORT_BATCH_DATA:
      return isIsSupportBatchData();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case LAST_INTERACTIVE_ID:
      return isSetLastInteractiveId();
    case LAST_INTERACTIVE_TIMESTAMP:
      return isSetLastInteractiveTimestamp();
    case LAST_INTERACTIVE_TYPE:
      return isSetLastInteractiveType();
    case IS_SUPPORT_BATCH_DATA:
      return isSetIsSupportBatchData();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof ExtraClientInfo)
      return this.equals((ExtraClientInfo)that);
    return false;
  }

  public boolean equals(ExtraClientInfo that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_lastInteractiveId = true && this.isSetLastInteractiveId();
    boolean that_present_lastInteractiveId = true && that.isSetLastInteractiveId();
    if (this_present_lastInteractiveId || that_present_lastInteractiveId) {
      if (!(this_present_lastInteractiveId && that_present_lastInteractiveId))
        return false;
      if (!this.lastInteractiveId.equals(that.lastInteractiveId))
        return false;
    }

    boolean this_present_lastInteractiveTimestamp = true && this.isSetLastInteractiveTimestamp();
    boolean that_present_lastInteractiveTimestamp = true && that.isSetLastInteractiveTimestamp();
    if (this_present_lastInteractiveTimestamp || that_present_lastInteractiveTimestamp) {
      if (!(this_present_lastInteractiveTimestamp && that_present_lastInteractiveTimestamp))
        return false;
      if (!this.lastInteractiveTimestamp.equals(that.lastInteractiveTimestamp))
        return false;
    }

    boolean this_present_lastInteractiveType = true && this.isSetLastInteractiveType();
    boolean that_present_lastInteractiveType = true && that.isSetLastInteractiveType();
    if (this_present_lastInteractiveType || that_present_lastInteractiveType) {
      if (!(this_present_lastInteractiveType && that_present_lastInteractiveType))
        return false;
      if (!this.lastInteractiveType.equals(that.lastInteractiveType))
        return false;
    }

    boolean this_present_isSupportBatchData = true && this.isSetIsSupportBatchData();
    boolean that_present_isSupportBatchData = true && that.isSetIsSupportBatchData();
    if (this_present_isSupportBatchData || that_present_isSupportBatchData) {
      if (!(this_present_isSupportBatchData && that_present_isSupportBatchData))
        return false;
      if (this.isSupportBatchData != that.isSupportBatchData)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetLastInteractiveId()) ? 131071 : 524287);
    if (isSetLastInteractiveId())
      hashCode = hashCode * 8191 + lastInteractiveId.hashCode();

    hashCode = hashCode * 8191 + ((isSetLastInteractiveTimestamp()) ? 131071 : 524287);
    if (isSetLastInteractiveTimestamp())
      hashCode = hashCode * 8191 + lastInteractiveTimestamp.hashCode();

    hashCode = hashCode * 8191 + ((isSetLastInteractiveType()) ? 131071 : 524287);
    if (isSetLastInteractiveType())
      hashCode = hashCode * 8191 + lastInteractiveType.hashCode();

    hashCode = hashCode * 8191 + ((isSetIsSupportBatchData()) ? 131071 : 524287);
    if (isSetIsSupportBatchData())
      hashCode = hashCode * 8191 + ((isSupportBatchData) ? 131071 : 524287);

    return hashCode;
  }

  @Override
  public int compareTo(ExtraClientInfo other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetLastInteractiveId()).compareTo(other.isSetLastInteractiveId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLastInteractiveId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.lastInteractiveId, other.lastInteractiveId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetLastInteractiveTimestamp()).compareTo(other.isSetLastInteractiveTimestamp());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLastInteractiveTimestamp()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.lastInteractiveTimestamp, other.lastInteractiveTimestamp);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetLastInteractiveType()).compareTo(other.isSetLastInteractiveType());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLastInteractiveType()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.lastInteractiveType, other.lastInteractiveType);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetIsSupportBatchData()).compareTo(other.isSetIsSupportBatchData());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetIsSupportBatchData()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.isSupportBatchData, other.isSupportBatchData);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("ExtraClientInfo(");
    boolean first = true;

    if (isSetLastInteractiveId()) {
      sb.append("lastInteractiveId:");
      if (this.lastInteractiveId == null) {
        sb.append("null");
      } else {
        sb.append(this.lastInteractiveId);
      }
      first = false;
    }
    if (isSetLastInteractiveTimestamp()) {
      if (!first) sb.append(", ");
      sb.append("lastInteractiveTimestamp:");
      if (this.lastInteractiveTimestamp == null) {
        sb.append("null");
      } else {
        sb.append(this.lastInteractiveTimestamp);
      }
      first = false;
    }
    if (isSetLastInteractiveType()) {
      if (!first) sb.append(", ");
      sb.append("lastInteractiveType:");
      if (this.lastInteractiveType == null) {
        sb.append("null");
      } else {
        sb.append(this.lastInteractiveType);
      }
      first = false;
    }
    if (isSetIsSupportBatchData()) {
      if (!first) sb.append(", ");
      sb.append("isSupportBatchData:");
      sb.append(this.isSupportBatchData);
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ExtraClientInfoStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ExtraClientInfoStandardScheme getScheme() {
      return new ExtraClientInfoStandardScheme();
    }
  }

  private static class ExtraClientInfoStandardScheme extends org.apache.thrift.scheme.StandardScheme<ExtraClientInfo> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ExtraClientInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // LAST_INTERACTIVE_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.lastInteractiveId = iprot.readString();
              struct.setLastInteractiveIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // LAST_INTERACTIVE_TIMESTAMP
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.lastInteractiveTimestamp = iprot.readString();
              struct.setLastInteractiveTimestampIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // LAST_INTERACTIVE_TYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.lastInteractiveType = iprot.readString();
              struct.setLastInteractiveTypeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // IS_SUPPORT_BATCH_DATA
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.isSupportBatchData = iprot.readBool();
              struct.setIsSupportBatchDataIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, ExtraClientInfo struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.lastInteractiveId != null) {
        if (struct.isSetLastInteractiveId()) {
          oprot.writeFieldBegin(LAST_INTERACTIVE_ID_FIELD_DESC);
          oprot.writeString(struct.lastInteractiveId);
          oprot.writeFieldEnd();
        }
      }
      if (struct.lastInteractiveTimestamp != null) {
        if (struct.isSetLastInteractiveTimestamp()) {
          oprot.writeFieldBegin(LAST_INTERACTIVE_TIMESTAMP_FIELD_DESC);
          oprot.writeString(struct.lastInteractiveTimestamp);
          oprot.writeFieldEnd();
        }
      }
      if (struct.lastInteractiveType != null) {
        if (struct.isSetLastInteractiveType()) {
          oprot.writeFieldBegin(LAST_INTERACTIVE_TYPE_FIELD_DESC);
          oprot.writeString(struct.lastInteractiveType);
          oprot.writeFieldEnd();
        }
      }
      if (struct.isSetIsSupportBatchData()) {
        oprot.writeFieldBegin(IS_SUPPORT_BATCH_DATA_FIELD_DESC);
        oprot.writeBool(struct.isSupportBatchData);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ExtraClientInfoTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ExtraClientInfoTupleScheme getScheme() {
      return new ExtraClientInfoTupleScheme();
    }
  }

  private static class ExtraClientInfoTupleScheme extends org.apache.thrift.scheme.TupleScheme<ExtraClientInfo> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ExtraClientInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetLastInteractiveId()) {
        optionals.set(0);
      }
      if (struct.isSetLastInteractiveTimestamp()) {
        optionals.set(1);
      }
      if (struct.isSetLastInteractiveType()) {
        optionals.set(2);
      }
      if (struct.isSetIsSupportBatchData()) {
        optionals.set(3);
      }
      oprot.writeBitSet(optionals, 4);
      if (struct.isSetLastInteractiveId()) {
        oprot.writeString(struct.lastInteractiveId);
      }
      if (struct.isSetLastInteractiveTimestamp()) {
        oprot.writeString(struct.lastInteractiveTimestamp);
      }
      if (struct.isSetLastInteractiveType()) {
        oprot.writeString(struct.lastInteractiveType);
      }
      if (struct.isSetIsSupportBatchData()) {
        oprot.writeBool(struct.isSupportBatchData);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ExtraClientInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(4);
      if (incoming.get(0)) {
        struct.lastInteractiveId = iprot.readString();
        struct.setLastInteractiveIdIsSet(true);
      }
      if (incoming.get(1)) {
        struct.lastInteractiveTimestamp = iprot.readString();
        struct.setLastInteractiveTimestampIsSet(true);
      }
      if (incoming.get(2)) {
        struct.lastInteractiveType = iprot.readString();
        struct.setLastInteractiveTypeIsSet(true);
      }
      if (incoming.get(3)) {
        struct.isSupportBatchData = iprot.readBool();
        struct.setIsSupportBatchDataIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

