#----------------------------------
# Paths and file system functions for FluentCassandra
#----------------------------------
root_folder = File.expand_path("#{File.dirname(__FILE__)}/..")

Folders = {
    :root => root_folder,
    :src => File.join(root_folder, "src"),
    :out => File.join(root_folder, "build"),
    :nuget_bin => File.join(root_folder, ".nuget"),
    :nuget_out => File.join(root_folder, "build", "nuget"),

    #Output folder for creating FluentCassandra nuget distributions
    :fluentcassandra_nuspec => {
        :root => File.join(root_folder, "build", "nuget", "FluentCassandra"),
        :lib => File.join(root_folder, "build", "nuget", "FluentCassandra", "lib"),
        :net40 => File.join(root_folder, "build", "nuget", "FluentCassandra", "lib", "net40"),
    },

    :fluentcassandra_symbol_nuspec => {
        :root => File.join(root_folder, "build", "nuget", "FluentCassandra-Symbol"),
        :lib => File.join(root_folder, "build", "nuget", "FluentCassandra-Symbol", "lib"),
        :src => File.join(root_folder, "build", "nuget", "FluentCassandra-Symbol", "src"),
        :net40 => File.join(root_folder, "build", "nuget", "FluentCassandra-Symbol", "lib", "net40"),
    },

    #specifies the locations of the binary DLLs we want to use in NuGet / XUnit
    :bin => {
        :fluentcassandra_net40 => 'placeholder - specify build environment',
    }
}

Files = {
    :solution => "FluentCassandra.sln",
    :version => "VERSION",
    :assembly_info => "SharedAssemblyInfo.cs",

    :fluentcassandra_net40 => {
        :bin => "#{Projects[:fluentcassandra_net40][:id]}.dll",
        :pdb => "#{Projects[:fluentcassandra_net40][:id]}.pdb"
    }
}

Commands = {
    :nuget => File.join(Folders[:nuget_bin], "NuGet.exe"),
}

#safe function for creating output directories
def create_dir(dirName)
    if !File.directory?(dirName)
        FileUtils.mkdir(dirName) #creates the /build directory
    end
end

#Deletes a directory from the tree (to keep the build folder clean)
def flush_dir(dirName)
    if File.directory?(dirName)
        FileUtils.remove_dir(dirName, true)
    end
end
