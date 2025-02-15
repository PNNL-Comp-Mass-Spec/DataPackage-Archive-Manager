// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Performance", "CA1866:Use char overload", Justification = "Ignore since not supported by .NET 4.8", Scope = "member", Target = "~M:DataPackage_Archive_Manager.Program.Main(System.String[])~System.Int32")]
[assembly: SuppressMessage("Style", "IDE0057:Use range operator", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:DataPackage_Archive_Manager.DataPackageArchiver.FindDataPackageFilesToArchive(DataPackage_Archive_Manager.DataPackageInfo,System.IO.DirectoryInfo,System.DateTime,MyEMSLReader.DataPackageListInfo,DataPackage_Archive_Manager.DataPackageArchiver.MyEMSLUploadInfo@)~System.Collections.Generic.List{Pacifica.Core.FileInfoObject}")]
[assembly: SuppressMessage("Style", "IDE0057:Use range operator", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:DataPackage_Archive_Manager.DataPackageArchiver.ParseDataPkgIDList(System.String)~System.Collections.Generic.List{System.Collections.Generic.KeyValuePair{System.Int32,System.Int32}}")]
[assembly: SuppressMessage("Style", "IDE0305:Simplify collection initialization", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:DataPackage_Archive_Manager.DataPackageArchiver.GetFilteredDataPackageInfoList(System.Collections.Generic.IEnumerable{DataPackage_Archive_Manager.DataPackageInfo},System.Collections.Generic.IEnumerable{System.Int32})~System.Collections.Generic.List{DataPackage_Archive_Manager.DataPackageInfo}")]
[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer .First", Scope = "member", Target = "~M:DataPackage_Archive_Manager.DataPackageArchiver.ProcessDataPackages(System.Collections.Generic.List{System.Collections.Generic.KeyValuePair{System.Int32,System.Int32}},System.DateTime)~System.Boolean")]
