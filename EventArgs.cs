﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataPackage_Archive_Manager
{
	public delegate void MessageEventHandler(object sender, MessageEventArgs e);
	public delegate void ProgressEventHandler(object sender, ProgressEventArgs e);

	public class MessageEventArgs : EventArgs
	{
		public readonly string Message;

		public MessageEventArgs(string message)
		{
			Message = message;
		}
	}

	public class ProgressEventArgs : EventArgs
	{
		/// <summary>
		/// Value between 0 and 100
		/// </summary>
		public readonly double PercentComplete;

		public ProgressEventArgs(double percentComplete)
		{
			PercentComplete = percentComplete;
		}
	}
}
