﻿using System;
using System.Collections;
using System.Diagnostics;
using System.Threading;

namespace FluentCassandra.Connections
{
	/// <summary>
	/// Thanks to Eric Gunnerson and Phil Haack
	/// </summary>
	internal struct TimedLock : IDisposable
	{
		private readonly object _target;

#if DEBUG
		private readonly Sentinel _leakDetector;
#endif

		/// <summary>
		/// Initializes a new instance of the <see cref="TimedLock"/> struct.
		/// </summary>
		/// <param name="o">
		/// The object to lock.
		/// </param>
		private TimedLock(object o)
		{
			_target = o;
#if DEBUG
			_leakDetector = new Sentinel();
#endif
		}

		/// <summary>
		/// Lock an object.
		/// </summary>
		/// <param name="o">The object to lock.</param>
		/// <returns></returns>
		public static TimedLock Lock(object o)
		{
			return Lock(o, TimeSpan.FromSeconds(10));
		}

		/// <summary>
		/// The object to lock.
		/// </summary>
		/// <param name="o">The object to lock.</param>
		/// <param name="timeout">The timeout.</param>
		/// <returns></returns>
		/// <exception cref="LockTimeoutException">
		/// </exception>
		public static TimedLock Lock(object o, TimeSpan timeout)
		{
			var tl = new TimedLock(o);

			if (!Monitor.TryEnter(o, timeout))
			{
#if DEBUG
				GC.SuppressFinalize(tl._leakDetector);
				StackTrace blockingTrace;
				lock (Sentinel.StackTraces)
				{
					blockingTrace = Sentinel.StackTraces[o] as StackTrace;
				}

				throw new LockTimeoutException(blockingTrace);
#else
				throw new LockTimeoutException();
#endif
			}
#if DEBUG

			// Lock acquired. Store the stack trace.
			var trace = new StackTrace();
			lock (Sentinel.StackTraces)
			{
				Sentinel.StackTraces.Add(o, trace);
			}

#endif
			return tl;
		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		public void Dispose()
		{
			Monitor.Exit(_target);

			// It's a bad error if someone forgets to call Dispose,
			// so in Debug builds, we put a finalizer in to detect
			// the error. If Dispose is called, we suppress the
			// finalizer.
#if DEBUG
			GC.SuppressFinalize(_leakDetector);
			lock (Sentinel.StackTraces)
			{
				Sentinel.StackTraces.Remove(_target);
			}

#endif
		}

#if DEBUG


		// (In Debug mode, we make it a class so that we can add a finalizer
		// in order to detect when the object is not freed.)
		/// <summary>
		/// The sentinel.
		/// </summary>
		private class Sentinel
		{
			/// <summary>
			/// The stack traces.
			/// </summary>
			public static readonly Hashtable StackTraces = new Hashtable();

			/// <summary>
			/// Finalizes an instance of the <see cref="Sentinel"/> class. 
			/// </summary>
			~Sentinel()
			{
				// If this finalizer runs, someone somewhere failed to
				// call Dispose, which means we've failed to leave
				// a monitor!
				Debug.Fail("Undisposed lock");
			}
		}
#endif
	}

	#region internal class LockTimeoutException : ApplicationException

    #endregion
}