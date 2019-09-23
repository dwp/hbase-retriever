rootProject.name = "HbaseRetriever"

buildCache {
    local<DirectoryBuildCache>{
        setDirectory(File(settingsDir, "build-cache"))
    }
}
