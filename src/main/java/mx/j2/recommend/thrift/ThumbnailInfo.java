/**
 * Autogenerated by Thrift Compiler (0.11.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package mx.j2.recommend.thrift;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.11.0)", date = "2021-06-02")
public class ThumbnailInfo implements org.apache.thrift.TBase<ThumbnailInfo, ThumbnailInfo._Fields>, java.io.Serializable, Cloneable, Comparable<ThumbnailInfo> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ThumbnailInfo");

  private static final org.apache.thrift.protocol.TField THUMBNAIL_URL_FIELD_DESC = new org.apache.thrift.protocol.TField("thumbnailUrl", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField WIDTH_FIELD_DESC = new org.apache.thrift.protocol.TField("width", org.apache.thrift.protocol.TType.I32, (short)2);
  private static final org.apache.thrift.protocol.TField HEIGHT_FIELD_DESC = new org.apache.thrift.protocol.TField("height", org.apache.thrift.protocol.TType.I32, (short)3);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new ThumbnailInfoStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new ThumbnailInfoTupleSchemeFactory();

  public java.lang.String thumbnailUrl; // required
  public int width; // required
  public int height; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    THUMBNAIL_URL((short)1, "thumbnailUrl"),
    WIDTH((short)2, "width"),
    HEIGHT((short)3, "height");

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
        case 1: // THUMBNAIL_URL
          return THUMBNAIL_URL;
        case 2: // WIDTH
          return WIDTH;
        case 3: // HEIGHT
          return HEIGHT;
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
  private static final int __WIDTH_ISSET_ID = 0;
  private static final int __HEIGHT_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.THUMBNAIL_URL, new org.apache.thrift.meta_data.FieldMetaData("thumbnailUrl", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.WIDTH, new org.apache.thrift.meta_data.FieldMetaData("width", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.HEIGHT, new org.apache.thrift.meta_data.FieldMetaData("height", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ThumbnailInfo.class, metaDataMap);
  }

  public ThumbnailInfo() {
  }

  public ThumbnailInfo(
    java.lang.String thumbnailUrl,
    int width,
    int height)
  {
    this();
    this.thumbnailUrl = thumbnailUrl;
    this.width = width;
    setWidthIsSet(true);
    this.height = height;
    setHeightIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ThumbnailInfo(ThumbnailInfo other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetThumbnailUrl()) {
      this.thumbnailUrl = other.thumbnailUrl;
    }
    this.width = other.width;
    this.height = other.height;
  }

  public ThumbnailInfo deepCopy() {
    return new ThumbnailInfo(this);
  }

  @Override
  public void clear() {
    this.thumbnailUrl = null;
    setWidthIsSet(false);
    this.width = 0;
    setHeightIsSet(false);
    this.height = 0;
  }

  public java.lang.String getThumbnailUrl() {
    return this.thumbnailUrl;
  }

  public ThumbnailInfo setThumbnailUrl(java.lang.String thumbnailUrl) {
    this.thumbnailUrl = thumbnailUrl;
    return this;
  }

  public void unsetThumbnailUrl() {
    this.thumbnailUrl = null;
  }

  /** Returns true if field thumbnailUrl is set (has been assigned a value) and false otherwise */
  public boolean isSetThumbnailUrl() {
    return this.thumbnailUrl != null;
  }

  public void setThumbnailUrlIsSet(boolean value) {
    if (!value) {
      this.thumbnailUrl = null;
    }
  }

  public int getWidth() {
    return this.width;
  }

  public ThumbnailInfo setWidth(int width) {
    this.width = width;
    setWidthIsSet(true);
    return this;
  }

  public void unsetWidth() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __WIDTH_ISSET_ID);
  }

  /** Returns true if field width is set (has been assigned a value) and false otherwise */
  public boolean isSetWidth() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __WIDTH_ISSET_ID);
  }

  public void setWidthIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __WIDTH_ISSET_ID, value);
  }

  public int getHeight() {
    return this.height;
  }

  public ThumbnailInfo setHeight(int height) {
    this.height = height;
    setHeightIsSet(true);
    return this;
  }

  public void unsetHeight() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __HEIGHT_ISSET_ID);
  }

  /** Returns true if field height is set (has been assigned a value) and false otherwise */
  public boolean isSetHeight() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __HEIGHT_ISSET_ID);
  }

  public void setHeightIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __HEIGHT_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, java.lang.Object value) {
    switch (field) {
    case THUMBNAIL_URL:
      if (value == null) {
        unsetThumbnailUrl();
      } else {
        setThumbnailUrl((java.lang.String)value);
      }
      break;

    case WIDTH:
      if (value == null) {
        unsetWidth();
      } else {
        setWidth((java.lang.Integer)value);
      }
      break;

    case HEIGHT:
      if (value == null) {
        unsetHeight();
      } else {
        setHeight((java.lang.Integer)value);
      }
      break;

    }
  }

  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case THUMBNAIL_URL:
      return getThumbnailUrl();

    case WIDTH:
      return getWidth();

    case HEIGHT:
      return getHeight();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case THUMBNAIL_URL:
      return isSetThumbnailUrl();
    case WIDTH:
      return isSetWidth();
    case HEIGHT:
      return isSetHeight();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof ThumbnailInfo)
      return this.equals((ThumbnailInfo)that);
    return false;
  }

  public boolean equals(ThumbnailInfo that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_thumbnailUrl = true && this.isSetThumbnailUrl();
    boolean that_present_thumbnailUrl = true && that.isSetThumbnailUrl();
    if (this_present_thumbnailUrl || that_present_thumbnailUrl) {
      if (!(this_present_thumbnailUrl && that_present_thumbnailUrl))
        return false;
      if (!this.thumbnailUrl.equals(that.thumbnailUrl))
        return false;
    }

    boolean this_present_width = true;
    boolean that_present_width = true;
    if (this_present_width || that_present_width) {
      if (!(this_present_width && that_present_width))
        return false;
      if (this.width != that.width)
        return false;
    }

    boolean this_present_height = true;
    boolean that_present_height = true;
    if (this_present_height || that_present_height) {
      if (!(this_present_height && that_present_height))
        return false;
      if (this.height != that.height)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetThumbnailUrl()) ? 131071 : 524287);
    if (isSetThumbnailUrl())
      hashCode = hashCode * 8191 + thumbnailUrl.hashCode();

    hashCode = hashCode * 8191 + width;

    hashCode = hashCode * 8191 + height;

    return hashCode;
  }

  @Override
  public int compareTo(ThumbnailInfo other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetThumbnailUrl()).compareTo(other.isSetThumbnailUrl());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetThumbnailUrl()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.thumbnailUrl, other.thumbnailUrl);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetWidth()).compareTo(other.isSetWidth());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetWidth()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.width, other.width);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetHeight()).compareTo(other.isSetHeight());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetHeight()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.height, other.height);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("ThumbnailInfo(");
    boolean first = true;

    sb.append("thumbnailUrl:");
    if (this.thumbnailUrl == null) {
      sb.append("null");
    } else {
      sb.append(this.thumbnailUrl);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("width:");
    sb.append(this.width);
    first = false;
    if (!first) sb.append(", ");
    sb.append("height:");
    sb.append(this.height);
    first = false;
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

  private static class ThumbnailInfoStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ThumbnailInfoStandardScheme getScheme() {
      return new ThumbnailInfoStandardScheme();
    }
  }

  private static class ThumbnailInfoStandardScheme extends org.apache.thrift.scheme.StandardScheme<ThumbnailInfo> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ThumbnailInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // THUMBNAIL_URL
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.thumbnailUrl = iprot.readString();
              struct.setThumbnailUrlIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // WIDTH
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.width = iprot.readI32();
              struct.setWidthIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // HEIGHT
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.height = iprot.readI32();
              struct.setHeightIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, ThumbnailInfo struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.thumbnailUrl != null) {
        oprot.writeFieldBegin(THUMBNAIL_URL_FIELD_DESC);
        oprot.writeString(struct.thumbnailUrl);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(WIDTH_FIELD_DESC);
      oprot.writeI32(struct.width);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(HEIGHT_FIELD_DESC);
      oprot.writeI32(struct.height);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ThumbnailInfoTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ThumbnailInfoTupleScheme getScheme() {
      return new ThumbnailInfoTupleScheme();
    }
  }

  private static class ThumbnailInfoTupleScheme extends org.apache.thrift.scheme.TupleScheme<ThumbnailInfo> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ThumbnailInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetThumbnailUrl()) {
        optionals.set(0);
      }
      if (struct.isSetWidth()) {
        optionals.set(1);
      }
      if (struct.isSetHeight()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetThumbnailUrl()) {
        oprot.writeString(struct.thumbnailUrl);
      }
      if (struct.isSetWidth()) {
        oprot.writeI32(struct.width);
      }
      if (struct.isSetHeight()) {
        oprot.writeI32(struct.height);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ThumbnailInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.thumbnailUrl = iprot.readString();
        struct.setThumbnailUrlIsSet(true);
      }
      if (incoming.get(1)) {
        struct.width = iprot.readI32();
        struct.setWidthIsSet(true);
      }
      if (incoming.get(2)) {
        struct.height = iprot.readI32();
        struct.setHeightIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}
