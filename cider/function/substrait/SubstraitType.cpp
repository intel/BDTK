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

#include "function/substrait/SubstraitType.h"
#include <sstream>

namespace cider::function::substrait {

namespace {

size_t findNextComma(const std::string& str, size_t start) {
  int cnt = 0;
  for (auto i = start; i < str.size(); i++) {
    if (str[i] == '<') {
      cnt++;
    } else if (str[i] == '>') {
      cnt--;
    } else if (cnt == 0 && str[i] == ',') {
      return i;
    }
  }

  return std::string::npos;
}

}  // namespace

SubstraitTypePtr SubstraitType::decode(const std::string& rawType) {
  std::string matchingType = rawType;
  const auto& questionMaskPos = rawType.find_last_of('?');
  // deal with type and with a question mask like "i32?".
  if (questionMaskPos != std::string::npos) {
    matchingType = rawType.substr(0, questionMaskPos);
  }
  std::transform(matchingType.begin(),
                 matchingType.end(),
                 matchingType.begin(),
                 [](unsigned char c) { return std::tolower(c); });

  const auto& leftAngleBracketPos = rawType.find('<');
  if (leftAngleBracketPos == std::string::npos) {
    const auto& scalarType = scalarTypeMapping().find(matchingType);
    if (scalarType != scalarTypeMapping().end()) {
      return scalarType->second;
    } else if (matchingType.rfind("unknown", 0) == 0) {
      return std::make_shared<const SubstraitUsedDefinedType>(rawType);
    } else {
      return std::make_shared<const SubstraitStringLiteralType>(rawType);
    }
  }
  const auto& rightAngleBracketPos = rawType.rfind('>');
  /*VELOX_CHECK(
      rightAngleBracketPos != std::string::npos,
      "Couldn't find the closing angle bracket.");*/

  auto baseType = matchingType.substr(0, leftAngleBracketPos);

  std::vector<SubstraitTypePtr> nestedTypes;
  nestedTypes.reserve(8);
  auto prevPos = leftAngleBracketPos + 1;
  auto commaPos = findNextComma(rawType, prevPos);
  while (commaPos != std::string::npos) {
    auto token = rawType.substr(prevPos, commaPos - prevPos);
    nestedTypes.emplace_back(decode(token));
    prevPos = commaPos + 1;
    commaPos = findNextComma(rawType, prevPos);
  }
  auto token = rawType.substr(prevPos, rightAngleBracketPos - prevPos);
  nestedTypes.emplace_back(decode(token));

  if (baseType == "list") {
    /*VELOX_CHECK(
        nestedTypes.size() == 1,
        "list type can only have one parameterized type");*/
    return std::make_shared<SubstraitListType>(nestedTypes[0]);
  } else if (baseType == "map") {
    /*VELOX_CHECK(
        nestedTypes.size() == 2,
        "map type must have a parameterized type for key and a parameterized type for
       value");*/
    return std::make_shared<SubstraitMapType>(nestedTypes[0], nestedTypes[1]);
  } else if (baseType == "decimal") {
    /*VELOX_CHECK(
        nestedTypes.size() == 2,
        "decimal type must have a parameterized type for precision and a parameterized
       type for scale");*/
    auto precision =
        std::dynamic_pointer_cast<const SubstraitStringLiteralType>(nestedTypes[0]);
    auto scale =
        std::dynamic_pointer_cast<const SubstraitStringLiteralType>(nestedTypes[1]);
    return std::make_shared<SubstraitDecimalType>(precision, scale);
  } else if (baseType == "varchar") {
    /*VELOX_CHECK(
        nestedTypes.size() == 1,
        "varchar type must have a parameterized type length");*/
    auto length =
        std::dynamic_pointer_cast<const SubstraitStringLiteralType>(nestedTypes[0]);
    return std::make_shared<SubstraitVarcharType>(length);
  } else if (baseType == "fixedchar") {
    /*VELOX_CHECK(
        nestedTypes.size() == 1,
        "fixedchar type must have a parameterized type length");*/
    auto length =
        std::dynamic_pointer_cast<const SubstraitStringLiteralType>(nestedTypes[0]);
    return std::make_shared<SubstraitFixedCharType>(length);
  } else if (baseType == "fixedbinary") {
    /*VELOX_CHECK(
        nestedTypes.size() == 1,
        "fixedbinary type must have a parameterized type length");*/
    auto length =
        std::dynamic_pointer_cast<const SubstraitStringLiteralType>(nestedTypes[0]);
    return std::make_shared<SubstraitFixedBinaryType>(length);
  } else if (baseType == "struct") {
    /*VELOX_CHECK(
        !nestedTypes.empty(),
        "struct type must have at least one parameterized type");*/
    return std::make_shared<SubstraitStructType>(nestedTypes);
  } else {
    // VELOX_NYI("Unsupported typed {}", rawType);
  }
}

#define SUBSTRAIT_SCALAR_TYPE_MAPPING(typeKind)                           \
  {                                                                       \
    SubstraitTypeTraits<SubstraitTypeKind::typeKind>::typeString,         \
        std::make_shared<SubstraitTypeBase<SubstraitTypeKind::typeKind>>( \
            SubstraitTypeBase<SubstraitTypeKind::typeKind>())             \
  }

const std::unordered_map<std::string, SubstraitTypePtr>&
SubstraitType::scalarTypeMapping() {
  static const std::unordered_map<std::string, SubstraitTypePtr> scalarTypeMap{
      SUBSTRAIT_SCALAR_TYPE_MAPPING(kBool),
      SUBSTRAIT_SCALAR_TYPE_MAPPING(kI8),
      SUBSTRAIT_SCALAR_TYPE_MAPPING(kI16),
      SUBSTRAIT_SCALAR_TYPE_MAPPING(kI32),
      SUBSTRAIT_SCALAR_TYPE_MAPPING(kI64),
      SUBSTRAIT_SCALAR_TYPE_MAPPING(kFp32),
      SUBSTRAIT_SCALAR_TYPE_MAPPING(kFp64),
      SUBSTRAIT_SCALAR_TYPE_MAPPING(kString),
      SUBSTRAIT_SCALAR_TYPE_MAPPING(kBinary),
      SUBSTRAIT_SCALAR_TYPE_MAPPING(kTimestamp),
      SUBSTRAIT_SCALAR_TYPE_MAPPING(kTimestampTz),
      SUBSTRAIT_SCALAR_TYPE_MAPPING(kDate),
      SUBSTRAIT_SCALAR_TYPE_MAPPING(kTime),
      SUBSTRAIT_SCALAR_TYPE_MAPPING(kIntervalDay),
      SUBSTRAIT_SCALAR_TYPE_MAPPING(kIntervalYear),
      SUBSTRAIT_SCALAR_TYPE_MAPPING(kUuid),
  };
  return scalarTypeMap;
}

const std::string SubstraitFixedBinaryType::signature() const {
  std::stringstream sign;
  sign << SubstraitTypeBase::signature();
  sign << "<";
  sign << length_->value();
  sign << ">";
  return sign.str();
}

bool SubstraitFixedBinaryType::isSameAs(
    const std::shared_ptr<const SubstraitType>& other) const {
  if (const auto& type =
          std::dynamic_pointer_cast<const SubstraitFixedBinaryType>(other)) {
    return true;
  }
  return false;
}

const std::string SubstraitDecimalType::signature() const {
  std::stringstream signature;
  signature << SubstraitTypeBase::signature();
  signature << "<";
  signature << precision_->value() << "," << scale_->value();
  signature << ">";
  return signature.str();
}

bool SubstraitDecimalType::isSameAs(
    const std::shared_ptr<const SubstraitType>& other) const {
  if (const auto& type = std::dynamic_pointer_cast<const SubstraitDecimalType>(other)) {
    return true;
  }
  return false;
}

const std::string SubstraitFixedCharType::signature() const {
  std::ostringstream sign;
  sign << SubstraitTypeBase::signature();
  sign << "<";
  sign << length_->value();
  sign << ">";
  return sign.str();
}

bool SubstraitFixedCharType::isSameAs(
    const std::shared_ptr<const SubstraitType>& other) const {
  if (const auto& type = std::dynamic_pointer_cast<const SubstraitFixedCharType>(other)) {
    return true;
  }
  return false;
}

const std::string SubstraitVarcharType::signature() const {
  std::ostringstream sign;
  sign << SubstraitTypeBase::signature();
  sign << "<";
  sign << length_->value();
  sign << ">";
  return sign.str();
}

bool SubstraitVarcharType::isSameAs(
    const std::shared_ptr<const SubstraitType>& other) const {
  if (const auto& type = std::dynamic_pointer_cast<const SubstraitVarcharType>(other)) {
    return true;
  }
  return false;
}

const std::string SubstraitStructType::signature() const {
  std::ostringstream signature;
  signature << SubstraitTypeBase::signature();
  signature << "<";
  for (auto it = children_.begin(); it != children_.end(); ++it) {
    const auto& typeSign = (*it)->signature();
    if (it == children_.end() - 1) {
      signature << typeSign;
    } else {
      signature << typeSign << ",";
    }
  }
  signature << ">";
  return signature.str();
}

bool SubstraitStructType::isSameAs(
    const std::shared_ptr<const SubstraitType>& other) const {
  if (const auto& type = std::dynamic_pointer_cast<const SubstraitStructType>(other)) {
    bool sameSize = type->children_.size() == children_.size();
    if (sameSize) {
      for (int i = 0; i < children_.size(); i++) {
        if (!children_[i]->isSameAs(type->children_[i])) {
          return false;
        }
      }
      return true;
    }
  }
  return false;
}

const std::string SubstraitMapType::signature() const {
  std::ostringstream signature;
  signature << SubstraitTypeBase::signature();
  signature << "<";
  signature << keyType_->signature();
  signature << ",";
  signature << valueType_->signature();
  signature << ">";
  return signature.str();
}

bool SubstraitMapType::isSameAs(const std::shared_ptr<const SubstraitType>& other) const {
  if (const auto& type = std::dynamic_pointer_cast<const SubstraitMapType>(other)) {
    return keyType_->isSameAs(type->keyType_) && valueType_->isSameAs(type->valueType_);
  }
  return false;
}

const std::string SubstraitListType::signature() const {
  std::ostringstream signature;
  signature << SubstraitTypeBase::signature();
  signature << "<";
  signature << type_->signature();
  signature << ">";
  return signature.str();
}

bool SubstraitListType::isSameAs(
    const std::shared_ptr<const SubstraitType>& other) const {
  if (const auto& type = std::dynamic_pointer_cast<const SubstraitListType>(other)) {
    return type_->isSameAs(type->type_);
  }
  return false;
}

bool SubstraitUsedDefinedType::isSameAs(
    const std::shared_ptr<const SubstraitType>& other) const {
  if (const auto& type =
          std::dynamic_pointer_cast<const SubstraitUsedDefinedType>(other)) {
    return type->value_ == value_;
  }
  return false;
}

bool SubstraitStringLiteralType::isSameAs(
    const std::shared_ptr<const SubstraitType>& other) const {
  if (isWildcard()) {
    return true;
  }
  if (const auto& type =
          std::dynamic_pointer_cast<const SubstraitStringLiteralType>(other)) {
    return type->value_ == value_;
  }
  return false;
}

#define DEFINE_SUBSTRAIT_SCALAR_ACCESSOR(typeKind)                                     \
  std::shared_ptr<const SubstraitScalarType<SubstraitTypeKind::typeKind>> typeKind() { \
    return std::make_shared<const SubstraitScalarType<SubstraitTypeKind::typeKind>>(); \
  }

DEFINE_SUBSTRAIT_SCALAR_ACCESSOR(kBool);
DEFINE_SUBSTRAIT_SCALAR_ACCESSOR(kI8);
DEFINE_SUBSTRAIT_SCALAR_ACCESSOR(kI16);
DEFINE_SUBSTRAIT_SCALAR_ACCESSOR(kI32);
DEFINE_SUBSTRAIT_SCALAR_ACCESSOR(kI64);
DEFINE_SUBSTRAIT_SCALAR_ACCESSOR(kFp32);
DEFINE_SUBSTRAIT_SCALAR_ACCESSOR(kFp64);
DEFINE_SUBSTRAIT_SCALAR_ACCESSOR(kString);
DEFINE_SUBSTRAIT_SCALAR_ACCESSOR(kBinary);
DEFINE_SUBSTRAIT_SCALAR_ACCESSOR(kTimestamp);
DEFINE_SUBSTRAIT_SCALAR_ACCESSOR(kDate);
DEFINE_SUBSTRAIT_SCALAR_ACCESSOR(kTime);
DEFINE_SUBSTRAIT_SCALAR_ACCESSOR(kIntervalYear);
DEFINE_SUBSTRAIT_SCALAR_ACCESSOR(kIntervalDay);
DEFINE_SUBSTRAIT_SCALAR_ACCESSOR(kTimestampTz);
DEFINE_SUBSTRAIT_SCALAR_ACCESSOR(kUuid);

#undef DEFINE_SUBSTRAIT_SCALAR_ACCESSOR

}  // namespace cider::function::substrait
