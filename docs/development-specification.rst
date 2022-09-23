Development Specification
===========================

1 Exception Specification
---------------------------

1.1 cider exception
+++++++++++++++++++++++++++

For exceptions in the cider directory, please import cider/include/cider/CiderException.h

Exception types such as CiderException, CiderRuntimeException, and CiderCompileException are defined,

please use the macro CIDER_THROW to throw exceptions, such as CIDER_THROW(CiderCompileException, "Exception message");

1.2 cider-velox Exception
+++++++++++++++++++++++++++

For exceptions in the cider-velox directory, please use thirdparty/velox/velox/common/base/Exceptions.h,

Exception types such as VeloxException, VeloxUserError, VeloxRuntimeError are defined,

please use the macros VELOX_UNSUPPORTED, VELOX_FAIL, VELOX_USER_FAIL, VELOX_NYI to throw exceptions, 

for example VELOX_UNSUPPORTED("Conversion is not supported yet, type is {}", type);