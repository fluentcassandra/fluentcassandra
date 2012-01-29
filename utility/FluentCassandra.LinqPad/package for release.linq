<Query Kind="Program">
  <Reference Relative="..\..\lib\Ionic.Zip.dll">C:\development\projects\FluentCassandra\lib\Ionic.Zip.dll</Reference>
  <Namespace>Ionic.Zip</Namespace>
</Query>

void Main()
{
	// get paths to everything
	var basePath = Path.GetDirectoryName(Util.CurrentQueryPath);
	var driver40Path = Path.Combine(basePath, @"bin\Release");
	var releaseDir = Path.Combine(basePath,"Release","");
	Directory.CreateDirectory(releaseDir);
	
	// get version from driver assembly
	var assemblyPath = Path.Combine(driver40Path, "FluentCassandra.LinqPad.dll");	
	var assembly = Assembly.LoadFile(assemblyPath);
	var version = assembly.GetName().Version.Dump("Version");	
	
	// zip up both release folders as lpx files	
	using (var ms = new MemoryStream())
	using (var releaseZip = new ZipFile())
	using (var lpx40 = new ZipFile())
	{
		var releaseZipPath = Path.Combine(releaseDir,string.Format("FluentCassandraDriver {0}.zip",version));	
		
		// 40 driver lpx file
		lpx40.AddFiles(GetFiles(driver40Path),"");
		lpx40.Save(ms);		
		ms.Seek(0, SeekOrigin.Begin);
		releaseZip.AddEntry("FluentCassandraDriver.lpx",ms);	
		
		// reset the memorystream	
		releaseZip.Save(releaseZipPath);
		ms.SetLength(0);	
		
		// readme
		releaseZip.AddFile(Path.Combine(basePath,"README.txt"),"");		
		releaseZip.Save(releaseZipPath);		
	}
	
	Process.Start(releaseDir);
}

IEnumerable<string> GetFiles(string dirPath)
{
	var di = new DirectoryInfo(dirPath);
	return di.GetFiles().Select(x => x.FullName);
}