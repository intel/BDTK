/*
 * Copyright (c) 2022 Intel Corporation.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef CIDER_FUNCTION_SUBSTRAIT_SUBSTRAITTYPE_H
#define CIDER_FUNCTION_SUBSTRAIT_SUBSTRAITTYPE_H

#include <iostream>
#include "substrait/algebra.pb.h"

namespace cider::function::substrait {

using SubstraitTypeKind = ::substrait::Type::KindCase;

template <SubstraitTypeKind KIND>
struct SubstraitTypeTraits {};

template <>
struct SubstraitTypeTraits<SubstraitTypeKind::kBool> {
  static constexpr const char* signature = "bool";
  static constexpr const char* typeString = "boolean";
};

template <>
struct SubstraitTypeTraits<SubstraitTypeKind::kI8> {
  static constexpr const char* signature = "i8";
  static constexpr const char* typeString = "i8";
};

template <>
struct SubstraitTypeTraits<SubstraitTypeKind::kI16> {
  static constexpr const char* signature = "i16";
  static constexpr const char* typeString = "i16";
};

template <>
struct SubstraitTypeTraits<SubstraitTypeKind::kI32> {
  static constexpr const char* signature = "i32";
  static constexpr const char* typeString = "i32";
};

template <>
struct SubstraitTypeTraits<SubstraitTypeKind::kI64> {
  static constexpr const char* signature = "i64";
  static constexpr const char* typeString = "i64";
};

template <>
struct SubstraitTypeTraits<SubstraitTypeKind::kFp32> {
  static constexpr const char* signature = "fp32";
  static constexpr const char* typeString = "fp32";
};

template <>
struct SubstraitTypeTraits<SubstraitTypeKind::kFp64> {
  static constexpr const char* signature = "fp64";
  static constexpr const char* typeString = "fp64";
};

template <>
struct SubstraitTypeTraits<SubstraitTypeKind::kString> {
  static constexpr const char* signature = "str";
  static constexpr const char* typeString = "string";
};

template <>
struct SubstraitTypeTraits<SubstraitTypeKind::kBinary> {
  static constexpr const char* signature = "vbin";
  static constexpr const char* typeString = "binary";
};

template <>
struct SubstraitTypeTraits<SubstraitTypeKind::kTimestamp> {
  static constexpr const char* signature = "ts";
  static constexpr const char* typeString = "timestamp";
};

template <>
struct SubstraitTypeTraits<SubstraitTypeKind::kTimestampTz> {
  static constexpr const char* signature = "tstz";
  static constexpr const char* typeString = "timestamp_tz";
};

template <>
struct SubstraitTypeTraits<SubstraitTypeKind::kDate> {
  static constexpr const char* signature = "date";
  static constexpr const char* typeString = "date";
};

template <>
struct SubstraitTypeTraits<SubstraitTypeKind::kTime> {
  static constexpr const char* signature = "time";
  static constexpr const char* typeString = "time";
};

template <>
struct SubstraitTypeTraits<SubstraitTypeKind::kIntervalYear> {
  static constexpr const char* signature = "iyear";
  static constexpr const char* typeString = "interval_year";
};

template <>
struct SubstraitTypeTraits<SubstraitTypeKind::kIntervalDay> {
  static constexpr const char* signature = "iday";
  static constexpr const char* typeString = "interval_day";
};

template <>
struct SubstraitTypeTraits<SubstraitTypeKind::kUuid> {
  static constexpr const char* signature = "uuid";
  static constexpr const char* typeString = "uuid";
};

template <>
struct SubstraitTypeTraits<SubstraitTypeKind::kFixedChar> {
  static constexpr const char* signature = "fchar";
  static constexpr const char* typeString = "fixedchar";
};

template <>
struct SubstraitTypeTraits<SubstraitTypeKind::kVarchar> {
  static constexpr const char* signature = "vchar";
  static constexpr const char* typeString = "varchar";
};

template <>
struct SubstraitTypeTraits<SubstraitTypeKind::kFixedBinary> {
  static constexpr const char* signature = "fbin";
  static constexpr const char* typeString = "fixedbinary";
};

template <>
struct SubstraitTypeTraits<SubstraitTypeKind::kDecimal> {
  static constexpr const char* signature = "dec";
  static constexpr const char* typeString = "decimal";
};

template <>
struct SubstraitTypeTraits<SubstraitTypeKind::kStruct> {
  static constexpr const char* signature = "struct";
  static constexpr const char* typeString = "struct";
};

template <>
struct SubstraitTypeTraits<SubstraitTypeKind::kList> {
  static constexpr const char* signature = "list";
  static constexpr const char* typeString = "list";
};

template <>
struct SubstraitTypeTraits<SubstraitTypeKind::kMap> {
  static constexpr const char* signature = "map";
  static constexpr const char* typeString = "map";
};

template <>
struct SubstraitTypeTraits<SubstraitTypeKind::kUserDefined> {
  static constexpr const char* signature = "u!name";
  static constexpr const char* typeString = "user defined type";
};

class SubstraitType {
 public:
  /// deserialize substrait raw type string into Substrait extension  type.
  /// @param rawType - substrait extension raw string type
  static std::shared_ptr<const SubstraitType> decode(const std::string& rawType);

  /// signature name of substrait type.
  virtual const std::string signature() const = 0;

  /// test type is a Wildcard type or not.
  virtual const bool isWildcard() const { return false; }

  /// unknown type,see @SubstraitUnknownType
  virtual const bool isUnknown() const { return false; }

  /// a known substrait type kind
  virtual const SubstraitTypeKind kind() const = 0;

  virtual const std::string typeString() const = 0;

  /// whether two types are same as each other
  virtual bool isSameAs(const std::shared_ptr<const SubstraitType>& other) const {
    return kind() == other->kind();
  }

 private:
  /// A map store the raw type string and corresponding Substrait Type
  static const std::unordered_map<std::string, std::shared_ptr<const SubstraitType>>&
  scalarTypeMapping();
};

using SubstraitTypePtr = std::shared_ptr<const SubstraitType>;

/// Types used in function argument declarations.
template <SubstraitTypeKind Kind>
class SubstraitTypeBase : public SubstraitType {
 public:
  const std::string signature() const override {
    return SubstraitTypeTraits<Kind>::signature;
  }

  virtual const SubstraitTypeKind kind() const override { return Kind; }

  const std::string typeString() const override {
    return SubstraitTypeTraits<Kind>::typeString;
  }
};

template <SubstraitTypeKind Kind>
class SubstraitScalarType : public SubstraitTypeBase<Kind> {};

/// A string literal type can present the 'any1'
class SubstraitStringLiteralType : public SubstraitType {
 public:
  SubstraitStringLiteralType(const std::string& value) : value_(value) {}

  const std::string& value() const { return value_; }

  const std::string signature() const override { return value_; }

  const std::string typeString() const override { return value_; }
  const bool isWildcard() const override {
    return value_.find("any") == 0 || value_ == "T";
  }

  bool isSameAs(const std::shared_ptr<const SubstraitType>& other) const override;

  const SubstraitTypeKind kind() const override {
    return SubstraitTypeKind ::KIND_NOT_SET;
  }

 private:
  /// raw string of wildcard type.
  const std::string value_;
};

using SubstraitStringLiteralTypePtr = std::shared_ptr<const SubstraitStringLiteralType>;

class SubstraitDecimalType : public SubstraitTypeBase<SubstraitTypeKind ::kDecimal> {
 public:
  SubstraitDecimalType(const SubstraitStringLiteralTypePtr& precision,
                       const SubstraitStringLiteralTypePtr& scale)
      : precision_(precision), scale_(scale) {}

  SubstraitDecimalType(const std::string& precision, const std::string& scale)
      : precision_(std::make_shared<SubstraitStringLiteralType>(precision))
      , scale_(std::make_shared<SubstraitStringLiteralType>(scale)) {}

  bool isSameAs(const std::shared_ptr<const SubstraitType>& other) const override;

  const std::string signature() const override;

  const std::string precision() const { return precision_->value(); }

  const std::string scale() const { return scale_->value(); }

 private:
  SubstraitStringLiteralTypePtr precision_;
  SubstraitStringLiteralTypePtr scale_;
};

class SubstraitFixedBinaryType
    : public SubstraitTypeBase<SubstraitTypeKind ::kFixedBinary> {
 public:
  SubstraitFixedBinaryType(const SubstraitStringLiteralTypePtr& length)
      : length_(length) {}

  bool isSameAs(const std::shared_ptr<const SubstraitType>& other) const override;

  const SubstraitStringLiteralTypePtr& length() const { return length_; }

  const std::string signature() const override;

 protected:
  SubstraitStringLiteralTypePtr length_;
};

class SubstraitFixedCharType : public SubstraitTypeBase<SubstraitTypeKind ::kFixedChar> {
 public:
  SubstraitFixedCharType(const SubstraitStringLiteralTypePtr& length) : length_(length) {}

  bool isSameAs(const std::shared_ptr<const SubstraitType>& other) const override;

  const SubstraitStringLiteralTypePtr& length() const { return length_; }

  const std::string signature() const override;

 protected:
  SubstraitStringLiteralTypePtr length_;
};

class SubstraitVarcharType : public SubstraitTypeBase<SubstraitTypeKind ::kVarchar> {
 public:
  SubstraitVarcharType(const SubstraitStringLiteralTypePtr& length) : length_(length) {}

  bool isSameAs(const std::shared_ptr<const SubstraitType>& other) const override;

  const SubstraitStringLiteralTypePtr& length() const { return length_; }

  const std::string signature() const override;

 protected:
  SubstraitStringLiteralTypePtr length_;
};

class SubstraitListType : public SubstraitTypeBase<SubstraitTypeKind ::kList> {
 public:
  SubstraitListType(const SubstraitTypePtr& child) : type_(child){};

  const SubstraitTypePtr type() const { return type_; }

  bool isSameAs(const std::shared_ptr<const SubstraitType>& other) const override;

  const std::string signature() const override;

 private:
  SubstraitTypePtr type_;
};

class SubstraitStructType : public SubstraitTypeBase<SubstraitTypeKind ::kStruct> {
 public:
  SubstraitStructType(const std::vector<SubstraitTypePtr>& types) : children_(types) {}

  bool isSameAs(const std::shared_ptr<const SubstraitType>& other) const override;

  const std::string signature() const override;

  const std::vector<SubstraitTypePtr>& children() const { return children_; }

 private:
  std::vector<SubstraitTypePtr> children_;
};

class SubstraitMapType : public SubstraitTypeBase<SubstraitTypeKind ::kMap> {
 public:
  SubstraitMapType(const SubstraitTypePtr& keyType, const SubstraitTypePtr& valueType)
      : keyType_(keyType), valueType_(valueType) {}

  const SubstraitTypePtr keyType() const { return keyType_; }

  const SubstraitTypePtr valueType() const { return valueType_; }

  bool isSameAs(const std::shared_ptr<const SubstraitType>& other) const override;

  const std::string signature() const override;

 private:
  SubstraitTypePtr keyType_;
  SubstraitTypePtr valueType_;
};

class SubstraitUsedDefinedType
    : public SubstraitTypeBase<SubstraitTypeKind ::kUserDefined> {
 public:
  SubstraitUsedDefinedType(const std::string& value) : value_(value) {}

  const std::string& value() const { return value_; }

  bool isSameAs(const std::shared_ptr<const SubstraitType>& other) const override;

  const bool isUnknown() const override { return "unknown" == value_; }

 private:
  /// raw string of wildcard type.
  const std::string value_;
};

struct SubstraitTypeAnchor {
  std::string uri;
  std::string name;

  bool operator==(const SubstraitTypeAnchor& other) const {
    return (uri == other.uri && name == other.name);
  }
};

using SubstraitTypeAnchorPtr = std::shared_ptr<SubstraitTypeAnchor>;

#define SUBSTRAIT_SCALAR_ACCESSOR(KIND) \
  std::shared_ptr<const SubstraitScalarType<SubstraitTypeKind::KIND>> KIND()

SUBSTRAIT_SCALAR_ACCESSOR(kBool);
SUBSTRAIT_SCALAR_ACCESSOR(kI8);
SUBSTRAIT_SCALAR_ACCESSOR(kI16);
SUBSTRAIT_SCALAR_ACCESSOR(kI32);
SUBSTRAIT_SCALAR_ACCESSOR(kI64);
SUBSTRAIT_SCALAR_ACCESSOR(kFp32);
SUBSTRAIT_SCALAR_ACCESSOR(kFp64);
SUBSTRAIT_SCALAR_ACCESSOR(kString);
SUBSTRAIT_SCALAR_ACCESSOR(kBinary);
SUBSTRAIT_SCALAR_ACCESSOR(kTimestamp);
SUBSTRAIT_SCALAR_ACCESSOR(kDate);
SUBSTRAIT_SCALAR_ACCESSOR(kTime);
SUBSTRAIT_SCALAR_ACCESSOR(kIntervalYear);
SUBSTRAIT_SCALAR_ACCESSOR(kIntervalDay);
SUBSTRAIT_SCALAR_ACCESSOR(kTimestampTz);
SUBSTRAIT_SCALAR_ACCESSOR(kUuid);

#undef SUBSTRAIT_SCALAR_ACCESSOR

}  // namespace cider::function::substrait

namespace std {
/// hash function of cider::function::substrait::SubstraitTypeAnchor
template <>
struct hash<cider::function::substrait::SubstraitTypeAnchor> {
  size_t operator()(const cider::function::substrait::SubstraitTypeAnchor& k) const {
    return hash<std::string>()(k.name) ^ hash<std::string>()(k.uri);
  }
};

};  // namespace std

#endif  // CIDER_FUNCTION_SUBSTRAIT_SUBSTRAITTYPE_H
