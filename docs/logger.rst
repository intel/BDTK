======
Logger
======

``#include "util/Logger.h"``

The BDTK Logger is based on `Boost.Log`_ with a design goal of being largely, though not completely, backward
compatible with `glog`_ in usage, but with additional control over the logging format and other features.

.. _Boost.Log: https://www.boost.org/libs/log/doc/html/index.html
.. _glog: https://github.com/google/glog

For the developer, log entries are made in a syntax that is similar to ``std::ostream``. Example::

    LOG(INFO) << "x = " << x;

where ``INFO`` is one common example of the log "severity" level. Other severities are ``WARNING``, ``FATAL``,
``DEBUG1``, etc. See `Severity`_.

In addition, there are a number of ``CHECK`` macros which act like ``assert()`` but will report via the
logging system upon failure, with an optional message, and subsequently call ``abort()``. Examples::

    CHECK(ptr);
    CHECK_LT(1u, list.size()) << "list must contain more than 1 element.";

If ``ptr==nullptr`` or ``1u >= list.size()`` then the program will abort, and log a corresponding ``FATAL`` message.
See `CHECK`_.

Initialization and Global Instances
-----------------------------------

To initialize the logging system, the function::

    namespace logger {

    void init(LogOptions const&);

    }

must be invoked with the ``logger::LogOptions`` object to be applied. It is recommended to run this from
``main()`` as early as possible. use the ``LOG``/``CHECK`` macros for all normal logging.

Usage
-----

Severity
^^^^^^^^

There are currently 8 severity levels that can be used with the ``LOG()`` macro, in decreasing order of
severity from most severe to least:

============ ============================================================================
**Severity** **When to Use**
``FATAL``    An unrecoverable error has occurred, and must be fixed in the software.
             This is the only severity which will result in a call to ``abort()``.
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

The ``CHECK(condition)`` macro evaluates ``condition`` as a boolean value. If true, then execution continues
with nothing logged. Otherwise both the ``condition`` source code string is logged at ``FATAL`` severity,
along with any optional ``<< message``, before calling ``abort()``. The program may then either exit, or
optionally catch the ``SIGABRT`` signal.

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
