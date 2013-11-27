#----------------------------------
# Project data for FluentCassandra
#----------------------------------

Projects = {
    :language => "en-US",
    :licenseUrl => "https://github.com/fluentcassandra/fluentcassandra/blob/master/LICENSE.txt",
    :projectUrl => "https://github.com/fluentcassandra/fluentcassandra",
    :iconUrl => "https://github.com/fluentcassandra/fluentcassandra/raw/master/nuget/FluentCassandra.Logo.png",

    :fluentcassandra_net40 => {
        :id => "FluentCassandra",
        :dir => "",
        :title => "Fluent Cassandra",
        :description => "FluentCassandra is a .NET library for accessing Cassandra, which wraps the Thrift client library and provides a more fluent POCO interface for accessing and querying the objects in Cassandra.",
        :copyright => "Copyright Nick Berardi, Managed Fusion, LLC 2011-2013",
        :authors => "Nick Berardi, Aaron Stannard",
        :company => "Managed Fusion, LLC",
        :nuget_tags => "fluent cassandra apache nosql database managedfusion .net40 cql cql3",
        :framework_assemblies => {
           :system_core => {
            :assemblyName => "System.Core",
            :targetFramework => "net40"
           },
           :system_numerics => {
            :assemblyName => "System.Numerics",
            :targetFramework => "net40"
           },
           :system_web => {
            :assemblyName => "System.Web",
            :targetFramework => "net40"
           }
        }
    }
}