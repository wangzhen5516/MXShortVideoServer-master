/**
 * Autogenerated by Thrift Compiler (0.11.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package mx.j2.recommend.thrift;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.11.0)", date = "2021-06-02")
public class Card implements org.apache.thrift.TBase<Card, Card._Fields>, java.io.Serializable, Cloneable, Comparable<Card> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Card");

  private static final org.apache.thrift.protocol.TField CARD_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("cardId", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField RESULT_LIST_FIELD_DESC = new org.apache.thrift.protocol.TField("resultList", org.apache.thrift.protocol.TType.LIST, (short)2);
  private static final org.apache.thrift.protocol.TField INTERNAL_USE_FIELD_DESC = new org.apache.thrift.protocol.TField("internalUse", org.apache.thrift.protocol.TType.STRUCT, (short)3);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new CardStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new CardTupleSchemeFactory();

  public java.lang.String cardId; // required
  public java.util.List<Result> resultList; // optional
  public InternalUse internalUse; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    CARD_ID((short)1, "cardId"),
    RESULT_LIST((short)2, "resultList"),
    INTERNAL_USE((short)3, "internalUse");

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
        case 1: // CARD_ID
          return CARD_ID;
        case 2: // RESULT_LIST
          return RESULT_LIST;
        case 3: // INTERNAL_USE
          return INTERNAL_USE;
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
  private static final _Fields optionals[] = {_Fields.RESULT_LIST,_Fields.INTERNAL_USE};
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.CARD_ID, new org.apache.thrift.meta_data.FieldMetaData("cardId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.RESULT_LIST, new org.apache.thrift.meta_data.FieldMetaData("resultList", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRUCT            , "Result"))));
    tmpMap.put(_Fields.INTERNAL_USE, new org.apache.thrift.meta_data.FieldMetaData("internalUse", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, InternalUse.class)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Card.class, metaDataMap);
  }

  public Card() {
  }

  public Card(
    java.lang.String cardId)
  {
    this();
    this.cardId = cardId;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Card(Card other) {
    if (other.isSetCardId()) {
      this.cardId = other.cardId;
    }
    if (other.isSetResultList()) {
      java.util.List<Result> __this__resultList = new java.util.ArrayList<Result>(other.resultList.size());
      for (Result other_element : other.resultList) {
        __this__resultList.add(new Result(other_element));
      }
      this.resultList = __this__resultList;
    }
    if (other.isSetInternalUse()) {
      this.internalUse = new InternalUse(other.internalUse);
    }
  }

  public Card deepCopy() {
    return new Card(this);
  }

  @Override
  public void clear() {
    this.cardId = null;
    this.resultList = null;
    this.internalUse = null;
  }

  public java.lang.String getCardId() {
    return this.cardId;
  }

  public Card setCardId(java.lang.String cardId) {
    this.cardId = cardId;
    return this;
  }

  public void unsetCardId() {
    this.cardId = null;
  }

  /** Returns true if field cardId is set (has been assigned a value) and false otherwise */
  public boolean isSetCardId() {
    return this.cardId != null;
  }

  public void setCardIdIsSet(boolean value) {
    if (!value) {
      this.cardId = null;
    }
  }

  public int getResultListSize() {
    return (this.resultList == null) ? 0 : this.resultList.size();
  }

  public java.util.Iterator<Result> getResultListIterator() {
    return (this.resultList == null) ? null : this.resultList.iterator();
  }

  public void addToResultList(Result elem) {
    if (this.resultList == null) {
      this.resultList = new java.util.ArrayList<Result>();
    }
    this.resultList.add(elem);
  }

  public java.util.List<Result> getResultList() {
    return this.resultList;
  }

  public Card setResultList(java.util.List<Result> resultList) {
    this.resultList = resultList;
    return this;
  }

  public void unsetResultList() {
    this.resultList = null;
  }

  /** Returns true if field resultList is set (has been assigned a value) and false otherwise */
  public boolean isSetResultList() {
    return this.resultList != null;
  }

  public void setResultListIsSet(boolean value) {
    if (!value) {
      this.resultList = null;
    }
  }

  public InternalUse getInternalUse() {
    return this.internalUse;
  }

  public Card setInternalUse(InternalUse internalUse) {
    this.internalUse = internalUse;
    return this;
  }

  public void unsetInternalUse() {
    this.internalUse = null;
  }

  /** Returns true if field internalUse is set (has been assigned a value) and false otherwise */
  public boolean isSetInternalUse() {
    return this.internalUse != null;
  }

  public void setInternalUseIsSet(boolean value) {
    if (!value) {
      this.internalUse = null;
    }
  }

  public void setFieldValue(_Fields field, java.lang.Object value) {
    switch (field) {
    case CARD_ID:
      if (value == null) {
        unsetCardId();
      } else {
        setCardId((java.lang.String)value);
      }
      break;

    case RESULT_LIST:
      if (value == null) {
        unsetResultList();
      } else {
        setResultList((java.util.List<Result>)value);
      }
      break;

    case INTERNAL_USE:
      if (value == null) {
        unsetInternalUse();
      } else {
        setInternalUse((InternalUse)value);
      }
      break;

    }
  }

  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case CARD_ID:
      return getCardId();

    case RESULT_LIST:
      return getResultList();

    case INTERNAL_USE:
      return getInternalUse();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case CARD_ID:
      return isSetCardId();
    case RESULT_LIST:
      return isSetResultList();
    case INTERNAL_USE:
      return isSetInternalUse();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof Card)
      return this.equals((Card)that);
    return false;
  }

  public boolean equals(Card that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_cardId = true && this.isSetCardId();
    boolean that_present_cardId = true && that.isSetCardId();
    if (this_present_cardId || that_present_cardId) {
      if (!(this_present_cardId && that_present_cardId))
        return false;
      if (!this.cardId.equals(that.cardId))
        return false;
    }

    boolean this_present_resultList = true && this.isSetResultList();
    boolean that_present_resultList = true && that.isSetResultList();
    if (this_present_resultList || that_present_resultList) {
      if (!(this_present_resultList && that_present_resultList))
        return false;
      if (!this.resultList.equals(that.resultList))
        return false;
    }

    boolean this_present_internalUse = true && this.isSetInternalUse();
    boolean that_present_internalUse = true && that.isSetInternalUse();
    if (this_present_internalUse || that_present_internalUse) {
      if (!(this_present_internalUse && that_present_internalUse))
        return false;
      if (!this.internalUse.equals(that.internalUse))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetCardId()) ? 131071 : 524287);
    if (isSetCardId())
      hashCode = hashCode * 8191 + cardId.hashCode();

    hashCode = hashCode * 8191 + ((isSetResultList()) ? 131071 : 524287);
    if (isSetResultList())
      hashCode = hashCode * 8191 + resultList.hashCode();

    hashCode = hashCode * 8191 + ((isSetInternalUse()) ? 131071 : 524287);
    if (isSetInternalUse())
      hashCode = hashCode * 8191 + internalUse.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(Card other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetCardId()).compareTo(other.isSetCardId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCardId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.cardId, other.cardId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetResultList()).compareTo(other.isSetResultList());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetResultList()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.resultList, other.resultList);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetInternalUse()).compareTo(other.isSetInternalUse());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetInternalUse()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.internalUse, other.internalUse);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("Card(");
    boolean first = true;

    sb.append("cardId:");
    if (this.cardId == null) {
      sb.append("null");
    } else {
      sb.append(this.cardId);
    }
    first = false;
    if (isSetResultList()) {
      if (!first) sb.append(", ");
      sb.append("resultList:");
      if (this.resultList == null) {
        sb.append("null");
      } else {
        sb.append(this.resultList);
      }
      first = false;
    }
    if (isSetInternalUse()) {
      if (!first) sb.append(", ");
      sb.append("internalUse:");
      if (this.internalUse == null) {
        sb.append("null");
      } else {
        sb.append(this.internalUse);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
    if (internalUse != null) {
      internalUse.validate();
    }
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
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class CardStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public CardStandardScheme getScheme() {
      return new CardStandardScheme();
    }
  }

  private static class CardStandardScheme extends org.apache.thrift.scheme.StandardScheme<Card> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, Card struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // CARD_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.cardId = iprot.readString();
              struct.setCardIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // RESULT_LIST
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list80 = iprot.readListBegin();
                struct.resultList = new java.util.ArrayList<Result>(_list80.size);
                Result _elem81;
                for (int _i82 = 0; _i82 < _list80.size; ++_i82)
                {
                  _elem81 = new Result();
                  _elem81.read(iprot);
                  struct.resultList.add(_elem81);
                }
                iprot.readListEnd();
              }
              struct.setResultListIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // INTERNAL_USE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.internalUse = new InternalUse();
              struct.internalUse.read(iprot);
              struct.setInternalUseIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, Card struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.cardId != null) {
        oprot.writeFieldBegin(CARD_ID_FIELD_DESC);
        oprot.writeString(struct.cardId);
        oprot.writeFieldEnd();
      }
      if (struct.resultList != null) {
        if (struct.isSetResultList()) {
          oprot.writeFieldBegin(RESULT_LIST_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.resultList.size()));
            for (Result _iter83 : struct.resultList)
            {
              _iter83.write(oprot);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      if (struct.internalUse != null) {
        if (struct.isSetInternalUse()) {
          oprot.writeFieldBegin(INTERNAL_USE_FIELD_DESC);
          struct.internalUse.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class CardTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public CardTupleScheme getScheme() {
      return new CardTupleScheme();
    }
  }

  private static class CardTupleScheme extends org.apache.thrift.scheme.TupleScheme<Card> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, Card struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetCardId()) {
        optionals.set(0);
      }
      if (struct.isSetResultList()) {
        optionals.set(1);
      }
      if (struct.isSetInternalUse()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetCardId()) {
        oprot.writeString(struct.cardId);
      }
      if (struct.isSetResultList()) {
        {
          oprot.writeI32(struct.resultList.size());
          for (Result _iter84 : struct.resultList)
          {
            _iter84.write(oprot);
          }
        }
      }
      if (struct.isSetInternalUse()) {
        struct.internalUse.write(oprot);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, Card struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.cardId = iprot.readString();
        struct.setCardIdIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list85 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.resultList = new java.util.ArrayList<Result>(_list85.size);
          Result _elem86;
          for (int _i87 = 0; _i87 < _list85.size; ++_i87)
          {
            _elem86 = new Result();
            _elem86.read(iprot);
            struct.resultList.add(_elem86);
          }
        }
        struct.setResultListIsSet(true);
      }
      if (incoming.get(2)) {
        struct.internalUse = new InternalUse();
        struct.internalUse.read(iprot);
        struct.setInternalUseIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

