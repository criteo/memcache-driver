using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

// General Information about an assembly is controlled through the following 
// set of attributes. Change these attribute values to modify the information
// associated with an assembly.
[assembly: AssemblyTitle("Criteo.Memcache")]
[assembly: AssemblyDescription("Memcache client by Criteo !")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("Criteo")]
[assembly: AssemblyProduct("Criteo.Memcache")]
[assembly: AssemblyCopyright("Copyright © Criteo 2012")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]

// Setting ComVisible to false makes the types in this assembly not visible 
// to COM components.  If you need to access a type in this assembly from 
// COM, set the ComVisible attribute to true on that type.
[assembly: ComVisible(false)]

// The following GUID is for the ID of the typelib if this project is exposed to COM
[assembly: Guid("1072ab76-cd3a-47e0-a4fe-f61bccb660a6")]

// Version information for an assembly consists of the following four values:
//
//      Major Version
//      Minor Version
//      Revision
//
// You can specify all the values or you can default the Build and Revision Numbers 
// by using the '*' as shown below:
// [assembly: AssemblyVersion("1.0.*")]


[assembly: AssemblyVersion(Criteo.Memcache.Version.ASSEMBLY_VERSION)]
[assembly: AssemblyFileVersion(Criteo.Memcache.Version.ASSEMBLY_FILE_VERSION)]
[assembly: AssemblyInformationalVersion(Criteo.Memcache.Version.ASSEMBLY_INFORMATIONAL_VERSION)]

[assembly: InternalsVisibleTo("Criteo.Memcache.UTest")]

namespace Criteo.Memcache
{
    internal static class Version
    {
        internal const string VERSION = "0.0.1";
        internal const string ASSEMBLY_VERSION = VERSION;
        internal const string ASSEMBLY_FILE_VERSION = VERSION;
        internal const string ASSEMBLY_INFORMATIONAL_VERSION = VERSION;
    }
}