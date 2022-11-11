Implementation Specification
=============================

Exception Specification
---------------------------

cider exception
+++++++++++++++++++++++++++

For exceptions in the cider directory, please import cider/include/cider/CiderException.h

Exception types such as CiderException, CiderRuntimeException, and CiderCompileException are defined,

please use the macro CIDER_THROW to throw exceptions, such as CIDER_THROW(CiderCompileException, "Exception message");

cider-velox exception
+++++++++++++++++++++++++++

For exceptions in the cider-velox directory, please use thirdparty/velox/velox/common/base/Exceptions.h,

Exception types such as VeloxException, VeloxUserError, VeloxRuntimeError are defined,

please use the macros VELOX_UNSUPPORTED, VELOX_FAIL, VELOX_USER_FAIL, VELOX_NYI to throw exceptions, 

for example VELOX_UNSUPPORTED("Conversion is not supported yet, type is {}", type);


CiderAllocator use guide
------------------------------------

introduction to CiderAllocator
+++++++++++++++++++++++++++++++++++

We provide the allocation interface in CiderAllocator.h and two default implementations

CiderDefaultAllocator provide the default allocate and deallocate function via system library

AlignAllocator provide the function to perform memory alignment


how to use
+++++++++++++++++++++++++++++++++++

user can construct an allocator from upstream memory pool, and then memory allocated will be in the pool.
or use the CiderDefaultAllocator, AlignAllocator if they don't want to do it, 
but this allocator must be present and passed throughout the calling logic
because Only in this way can you know the memory usage during the entire operation for memory managerment

A typical usage in CiderRunTimeModule

1. add the member variables in .h file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``std::shared_ptr<CiderAllocator> allocator``
 and add it in the constructor::
    
    CiderRuntimeModule(
      std::shared_ptr<CiderCompilationResult> ciderCompilationResult,
      CiderCompilationOption ciderCompilationOption = CiderCompilationOption::defaults(),
      CiderExecutionOption ciderExecutionOption = CiderExecutionOption::defaults(),
      std::shared_ptr<CiderAllocator> allocator = std::make_shared<CiderDefaultAllocator>());

2. in .cpp implementation class
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

::

    CiderRuntimeModule::CiderRuntimeModule(
        std::shared_ptr<CiderCompilationResult> ciderCompilationResult,
        CiderCompilationOption ciderCompilationOption,
        CiderExecutionOption ciderExecutionOption,
        std::shared_ptr<CiderAllocator> allocator)
        : ciderCompilationResult_(ciderCompilationResult)
        , ciderCompilationOption_(ciderCompilationOption)
        , ciderExecutionOption_(ciderExecutionOption)
        , allocator_(allocator)

3. Then we can use it in the implementation class for memory allocation and release
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

::
    
    const int8_t** col_buffers = reinterpret_cast<const int8_t**>(allocator_->allocate(sizeof(int8_t**) * (total_col_num)));
    allocator_->deallocate(reinterpret_cast<int8_t*>(col_buffers),sizeof(int8_t**) * (total_col_num));

4. Finally we can pass a custom allocator when creating CiderRunTimeModule or use the default
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

::

    auto customAllocator = std::make_shared<CustomAllocator>();
    cider_runtime_module_ = std::make_shared<CiderRuntimeModule>(compile_res_, compile_option, exe_option, customAllocator);


Logger
------------------------------------

``#include "util/Logger.h"``

The BDTK Logger is based on `Boost.Log`_ with a design goal of being largely, though not completely, backward
compatible with `glog`_ in usage, but with additional control over the logging format and other features.

.. _Boost.Log: https://www.boost.org/libs/log/doc/html/index.html
.. _glog: https://github.com/google/glog

For the developer, log entries are made in a syntax that is similar to ``std::ostream``. Example::

    LOG(INFO) << "x = " << x;

where ``INFO`` is one common example of the log "severity" level. Other severities are ``WARNING``, ``FATAL``,
``DEBUG1``, etc. See `Severity`_.

Initialization and Global Instances
++++++++++++++++++++++++++++++++++++++++

To initialize the logging system, the function::

    namespace logger {

    void init(LogOptions const&);

    }

LogOptions has the following public member variables and member functions:

  - ``boost::filesystem::path log_dir_{"bdtk_log"};`` // Logging directory. May be relative to data directory, or absolute.
  - ``std::string file_name_pattern_{".{SEVERITY}.%Y%m%d-%H%M%S.log"};`` // file_name_pattern is prepended with base_name.
  - ``std::string symlink_{".{SEVERITY}"};`` // symlink is prepended with base_name.
  - ``Severity severity_{Severity::INFO};``  // log level.
  - ``Severity severity_clog_{Severity::ERROR};`` //Log to console severity level.
  - ``bool auto_flush_{true};`` // Flush logging buffer to file after each message.
  - ``size_t max_files_{100};`` // The maximum number of log files, if it is 0, no log will be recorded
  - ``size_t min_free_space_{20 << 20};`` // Minimum number of bytes left on device before oldest log files are deleted.
  - ``size_t rotation_size_{10 << 20};`` // Maximum file size in bytes before new log files are started.
  - ``LogOptions(char const* argv0);``
  - ``boost::filesystem::path full_log_dir() const;`` // get log path
  - ``void set_base_path(std::string const& base_path);`` // set log base path
  - ``void parse_command_line(int, char const* const*);`` // Pass parameters using the command line

    + ``--log-directory`` : Logging directory. May be relative to data directory, or absolute.
    + ``--log-file-name`` : Log file name relative to log-directory.
    + ``--log-symlink`` : Symlink to active log.
    + ``--log-severity`` : Log to file severity level.
    + ``--log-severity-clog`` : Log to console severity level.
    + ``--log-auto-flush`` : Flush logging buffer to file after each message.
    + ``--log-max-files`` : Maximum number of log files to keep.
    + ``--log-min-free-space`` : Minimum number of bytes left on device before oldest log files are deleted.
    + ``--log-rotation-size`` : Maximum file size in bytes before new log files are started.

must be invoked with the ``logger::LogOptions`` object to be applied. It is recommended 
to run this from ``main()`` as early as possible. use the ``LOG``/``CHECK`` macros for all normal logging. Example::

  logger::LogOptions log_options(argv[0]);
  log_options.parse_command_line(argc, argv);
  log_options.max_files_ = 0;
  log_options.set_base_path("/root/work");
  log_options.log_dir_ = "bdtk_log";
  logger::init(log_options);

Usage
++++++++

Severity
^^^^^^^^

There are currently 8 severity levels that can be used with the ``LOG()`` macro, in decreasing order of
severity from most severe to least:

============ ============================================================================
**Severity** **When to Use**
``FATAL``    An unrecoverable error has occurred, and must be fixed in the software.
``ERROR``    A recoverable error has occurred, and must be fixed in the software.
``WARNING``  Something that "should not have" happened happened, but is not as demanding
             of an immediate fix as an ``ERROR``. Example: A deprecated feature is still
             being used, even though the user was informed of its deprecation.
``INFO``     Significant and informative milestones in the execution of the program.
             One must balance logging useful and informative information, against
             logging too much useless and redundant information that drowns the signal
             out with noise.
``DEBUG1``   More detailed information about the execution of the program than ``INFO``
             that would be useful for debugging, but less detailed than the below debug
             levels. E.g. don't log every row of a million-row table.
``DEBUG2``   More detailed information than ``DEBUG1``.
``DEBUG3``   More detailed information than ``DEBUG2``.
``DEBUG4``   More detailed information than ``DEBUG3``.
============ ============================================================================

All ``LOG()`` calls with a lesser severity are ignored and not logged. For example if ``LOG(DEBUG1)`` then log
calls ``LOG(DEBUG1)``, ``LOG(INFO)``, ``LOG(WARNING)``, ``LOG(ERROR)``, and ``LOG(FATAL)`` are active and
will produce log entries when executed, and log calls ``LOG(DEBUG4)``, ``LOG(DEBUG3)``, and ``LOG(DEBUG2)``
are ignored. If a function is called in the input stream, e.g. ``LOG(DEBUG2) << f(x)``, then ``f(x)`` will
be invoked if any only if ``DEBUG2`` is an active log severity.

Errors that are logged with ``ERROR`` and ``FATAL`` should be considered software errors, and not user errors.
For example, if the user inputs a malformed SQL query, e.g. ``SELEKT`` instead of ``SELECT``, then this
should NOT be logged as an ``ERROR``, but instead logged as an ``INFO`` with an appropriate response to
the user. This would be considered correct behavior of the software. In contrast, anytime an ``ERROR`` or
``FATAL`` is logged, then it means there is a bug in the software that must be fixed.

Log Files
^^^^^^^^^

A separate log file is produced for ``INFO``, ``WARNING``, ``ERROR``, and ``FATAL`` if the log severity is
active.  If any of the ``DEBUG`` severities are active, they are included into the ``INFO`` log file.  Each log
file redundantly includes all entries that are more severe than itself. For example if ``LOG(DEBUG1)``
then the ``INFO`` log file will include all log calls ``LOG(DEBUG1)``, ``LOG(INFO)``, ``LOG(WARNING)``,
``LOG(ERROR)``, and ``LOG(FATAL)``; the ``WARNING`` log file will include all log calls ``LOG(WARNING)``,
``LOG(ERROR)``, and ``LOG(FATAL)``.

The name of the log file by default includes the program name, severity, and timestamp of when its first entry
was made. Example::

    cider.INFO.20220928-162525.log

New log files are started on each (re)start. A symbolic link without the timestamp and file extension
suffix points to the latest version. Example::

    cider.INFO -> cider.INFO.20220928-162525.log

Format
""""""

The general format of a log entry is::

    (timestamp) (severity) (process_id) (query_id) (thread_id) (filename:line_number) (message)

Example::

    2019-09-18T16:25:25.659248 I 26481 0 0 measure.h:80 Timer start

Field descriptions:

| 1. Timestamp in local timezone with microsecond resolution.
| 2. Single-character severity level. In same order as above severity levels:
|    ``F`` ``E`` ``W`` ``I`` ``1`` ``2`` ``3`` ``4``
|    For instance the ``I`` implies that the above log entry is of ``INFO`` severity.
| 3. The `process_id` assigned by the operating system.
| 4. The `query_id` is a unique 64-bit positive integer incrementally assigned to each new SQL query. A value of `0` indicates that the log line is outside of the context of any particular query, or that the `query_id` is not available.
| 5. The `thread_id` is a unique 64-bit positive integer incrementally assigned to each new thread. `thread_id=1` is assigned to the first thread each time the program starts.
| 6. Source filename:Line number.
| 7. Custom message sent to ``LOG()`` via the insertion ``<<`` operator.

Note that log entries can contain line breaks, thus not all log lines will begin with these fields if
the message itself contains multiple lines.

Macros
^^^^^^

LOG
"""

In addition to the ``LOG()`` macro, there are:

 * ``LOG_IF(severity, condition)`` - Same as ``LOG(severity)`` but first checks a boolean ``condition`` and logs
   only if evaluated to ``true``.
 * ``VLOG(n)`` - Same as ``LOG(DEBUGn)`` for ``n = 1, 2, 3, 4``.

CHECK
"""""
In release mode, the ``CHECK`` macro does nothing. In debug mode,
the ``CHECK(condition)`` macro evaluates ``condition`` as a boolean value. If true, then execution continues
with nothing logged. Otherwise both the ``condition`` source code string is logged at ``FATAL`` severity,
along with any optional ``<< message``.

| Similarly there are 6 binary ``CHECK`` comparison macros:
| ``CHECK_EQ``, ``CHECK_NE``, ``CHECK_LT``, ``CHECK_LE``, ``CHECK_GT``, ``CHECK_GE``
| which accept two parameters, and apply the comparison operators ``==``, ``!=``, ``<``, ``<=``, ``>``, ``>=``, respectively. For example, ``CHECK_LT(1u, list.size())`` will evaluate ``1u < list.size()``, and log and ``abort()`` if not true. The advantage of calling ``CHECK_LT(1u, list.size())`` over ``CHECK(1u < list.size())`` is that the value of both operands will be logged if the test fails, which is not reported with ``CHECK()``.

DEBUG_TIMER
"""""""""""

``DebugTimer`` objects can be instantiated in the code that measure and log the duration of their own lifetimes,
and include the following features:

* Globally accessible via a macro. E.g. ``auto timer = DEBUG_TIMER(__func__)``.
* Single multi-line log entry is reported for nested timers.
* Enabled with the ``--enable-debug-timer`` program option. Without it, the ``timer`` objects have no effect.
* Include timers from spawned threads. Requires a call on the child thread informing the parent thread id:
  ``DEBUG_TIMER_NEW_THREAD(parent_thread_id);``

Example::

    void foo() {
      auto timer = DEBUG_TIMER(__func__);
      ...
      bar();
      ...
    }

    void bar() {
      auto timer = DEBUG_TIMER(__func__);
      ...
      bar2();
      ...
      timer.stop();  // Manually stop timer for bar().
      ...
    }

    void bar2() {
      auto timer = DEBUG_TIMER(__func__);
      ...
    }

Upon the destruction of the ``timer`` object within ``foo()``, a log entry similar to the following will be made::

    2019-10-17T15:22:53.981002 I 8980 foobar.cpp:70 DEBUG_TIMER thread_id(140719710320384)
    19ms total duration for foo
      17ms start(10ms) bar foobar.cpp:100
        13ms start(10ms) bar2 foobar.cpp:130

Fields for the ``Duration`` lines (last two line above) are:

#. Lifetime of ``timer`` object.
#. Time after start of current thread. (This can be used to find gaps in timing coverage.)
#. String parameter to ``DEBUG_TIMER`` (``__func__`` in above examples.)
#. File\:Line where ``DEBUG_TIMER`` was called from.

The first root ``DEBUG_TIMER`` instance is in ``foo()``, and the two others in ``bar()`` and ``bar2()`` are initiated
upon subsequent calls into the call stack, represented by the indentations.  Once the first root ``timer`` object
destructs, the entire ``DurationTree`` of recorded times are logged together into a single multi-line log entry,
one line per ``timer`` instance.

There is a ``DebugTimer::stop()`` method that manually stops the timer, serving the same function
as the destructor. The destructor then will have no further effect.

To embed timers in a spawned child thread, call ``DEBUG_TIMER_NEW_THREAD(parent_thread_id);`` from the child
thread. The ``parent_thread_id`` must get its value from ``logger::thread_id()`` before the new thread is spawned.
This will not start a timer, but will record the child-parent relationship so that subsequent ``DEBUG_TIMER``
calls are stored in the correct node of the parent tree.

.. note::

    Any timer that is created in a thread when no other timers are active in the same or parent thread is
    called a *root timer*. The timer stack is logged when the root timer destructs, or ``stop()`` is called,
    after which memory used for tracking the timer trees are freed.  The performance cost of this should be
    kept in mind when placing timers within the code.

.. warning::

    Non-root timers that end *after* their root timer ends will result in a **segmentation fault** . This is easily avoided by not interleaving timer
    lifetimes with one another in the same block of code, and making sure that all child threads end prior
    to the ending of any corresponding root timer.
    