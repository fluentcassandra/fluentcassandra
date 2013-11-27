$: << './'
require "rubygems"
require "bundler"
Bundler.setup

require 'albacore'
require 'version_bumper'

#-----------------------
# Local dependencies
#-----------------------
require File.expand_path(File.dirname(__FILE__)) + '/buildscripts/projects'
require File.expand_path(File.dirname(__FILE__)) + '/buildscripts/paths'

#-----------------------
# Environment variables
#-----------------------
@env_buildconfigname = "Release"

def env_buildversion
    bumper_version.to_s
end

def env_nuget_version
    version = env_buildversion.split(".")
    "#{version[0]}.#{version[1]}.#{version[2]}"
end

#-----------------------
# Control Flow (meant to be called directly)
#-----------------------

desc "Creates a new Release build of FluentCassandra locally"
task :default => [:build]

desc "Creates a new Debug build of FluentCassandra locally"
task :debug => [:set_debug_config, :build]

desc "Packs a Release build of FluentCassandra for NuGet"
task :nuget => [:build, :pack, :pack_symbol]

desc "Packs a Debug build of FluentCassandra for NuGet"
task :nuget_debug => [:debug, :pack, :pack_symbol]

#-----------------------
# Environment variables
#-----------------------
desc "Sets the build environment to Debug"
task :set_debug_config do
    @env_buildconfigname = "Debug"
end

#-----------------------
# MSBuild
#-----------------------

desc "Does a release build of everything in the solution"
msbuild :build => [:assemblyinfo] do |msb|
    msb.properties :configuration => @env_buildconfigname
    msb.targets :Clean, :Build #Does the equivalent of a "Rebuild Solution"
    msb.solution = File.join(Folders[:root], Files[:solution])
end

#-----------------------
# Version Management
#-----------------------

assemblyinfo :assemblyinfo do |asm|
    assemblyInfoPath = File.join(Folders[:root], Files[:assembly_info])

    asm.input_file = assemblyInfoPath
    asm.output_file = assemblyInfoPath

    asm.version = env_buildversion
    asm.file_version = env_buildversion
end

desc "Increments the build number for the project"
task :bump_build_number do
    bumper_version.bump_build
    bumper_version.write(File.join(Folders[:root], Files[:version]))
end

desc "Increments the revision number for the project"
task :bump_revision_number do
    bumper_version.bump_revision
    bumper_version.write(File.join(Folders[:root], Files[:version]))
end

desc "Increments the minor version number for the project"
task :bump_minor_version_number do
    bumper_version.bump_minor
    bumper_version.write(File.join(Folders[:root], Files[:version]))
end

desc "Increments the major version number for the project"
task :bump_major_version_number do
    bumper_version.bump_major
    bumper_version.write(File.join(Folders[:root], Files[:version]))
end

#-----------------------
# Output
#-----------------------
desc "Sets the output / bin folders based on the current build configuration"
task :set_output_folders do
    #.NET 4.0
    Folders[:bin][:fluentcassandra_net40] = File.join(Folders[:src], Projects[:fluentcassandra_net40][:dir],"bin", @env_buildconfigname)
end

desc "Wipes out the build folder so we have a clean slate to work with"
task :clean_output_folders => :set_output_folders do
    puts "Flushing build folder..."
    flush_dir(Folders[:nuget_out])
end

desc "Creates all of the output folders we need for ILMerge / NuGet"
task :create_output_folders => :clean_output_folders do
    create_dir(Folders[:out])

    #Nuget folders
    create_dir(Folders[:nuget_out])
    create_dir(Folders[:fluentcassandra_nuspec][:root])
    create_dir(Folders[:fluentcassandra_nuspec][:lib])
    create_dir(Folders[:fluentcassandra_nuspec][:net40])

    create_dir(Folders[:fluentcassandra_symbol_nuspec][:root])
    create_dir(Folders[:fluentcassandra_symbol_nuspec][:lib])
    create_dir(Folders[:fluentcassandra_symbol_nuspec][:src])
    create_dir(Folders[:fluentcassandra_symbol_nuspec][:net40])
end

#-----------------------
# NuGet Output
#-----------------------
output :fluentcassandra_net40_nuget_output => [:create_output_folders] do |out|
    out.from Folders[:bin][:fluentcassandra_net40]
    out.to Folders[:fluentcassandra_nuspec][:net40]
    out.file Files[:fluentcassandra_net40][:bin]
end

output :fluentcassandra_symbol_nuget_output => [:create_output_folders] do |out|
    out.from Folders[:bin][:fluentcassandra_net40]
    out.to Folders[:fluentcassandra_symbol_nuspec][:net40]
    out.file Files[:fluentcassandra_net40][:bin]
    out.file Files[:fluentcassandra_net40][:pdb]
end

task :fluentcassandra_symbol_src_nuget_output => [:create_output_folders] do |out|
    src = File.join(Folders[:src], Projects[:fluentcassandra_net40][:dir])
    dest = Folders[:fluentcassandra_symbol_nuspec][:src]
    FileUtils.cp_r Dir.glob(src + '/*.cs'), dest
    FileUtils.cp_r File.join(src, "Apache"), dest
    FileUtils.cp_r File.join(src, "Connections"), dest
    FileUtils.cp_r File.join(src, "Linq"), dest
    FileUtils.cp_r File.join(src, "ObjectSerializer"), dest
    FileUtils.cp_r File.join(src, "Operations"), dest
    FileUtils.cp_r File.join(src, "Properties"), dest
    FileUtils.cp_r File.join(src, "System"), dest
    FileUtils.cp_r File.join(src, "Thrift"), dest
    FileUtils.cp_r File.join(src, "Types"), dest
end

desc "Executes all file/copy tasks"
task :all_output => [:fluentcassandra_net40_nuget_output, :fluentcassandra_symbol_nuget_output, :fluentcassandra_symbol_src_nuget_output]

#-----------------------
# NuSpec
#-----------------------
desc "Builds a nuspec file for FluentCassandra"
nuspec :nuspec => [:all_output] do |nuspec|
    nuspec.id = Projects[:fluentcassandra_net40][:id]
    nuspec.title = Projects[:fluentcassandra_net40][:title]
    nuspec.version = env_nuget_version
    nuspec.authors = Projects[:fluentcassandra_net40][:authors]
    nuspec.owners = Projects[:fluentcassandra_net40][:company]
    nuspec.description = Projects[:fluentcassandra_net40][:description]
    nuspec.iconUrl = Projects[:iconUrl]
    nuspec.projectUrl = Projects[:projectUrl]
    nuspec.licenseUrl = Projects[:licenseUrl]
    #nuspec.require_license_acceptance = false #causes an issue with Albacore 0.3.5
    nuspec.language = Projects[:language]
    nuspec.tags = Projects[:fluentcassandra_net40][:nuget_tags]
    nuspec.output_file = File.join(Folders[:nuget_out], "#{Projects[:fluentcassandra_net40][:id]}-v#{env_nuget_version}(#{@env_buildconfigname}).nuspec");

    #framework assemblies
    Projects[:fluentcassandra_net40][:framework_assemblies].each do |key, array|
        nuspec.framework_assembly array[:assemblyName], array[:targetFramework]
    end
end

#-----------------------
# NuGet Pack
#-----------------------
desc "Packs a build of FluentCassandra into a NuGet package"
nugetpack :pack => [:nuspec] do |nuget|
    nuget.command = Commands[:nuget]
    nuget.nuspec = File.join(Folders[:nuget_out], "#{Projects[:fluentcassandra_net40][:id]}-v#{env_nuget_version}(#{@env_buildconfigname}).nuspec")
    nuget.base_folder = Folders[:fluentcassandra_nuspec][:root]
    nuget.output = Folders[:nuget_out]
end

desc "Packs a symbol build of FluentCassandra into a NuGet package"
nugetpack :pack_symbol => [:nuspec] do |nuget|
    nuget.command = Commands[:nuget]
    nuget.nuspec = File.join(Folders[:nuget_out], "#{Projects[:fluentcassandra_net40][:id]}-v#{env_nuget_version}(#{@env_buildconfigname}).nuspec")
    nuget.base_folder = Folders[:fluentcassandra_symbol_nuspec][:root]
    nuget.output = Folders[:nuget_out]
    nuget.symbols = true
end