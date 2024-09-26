rm -f archive.tar.gz
tar -czvf archive.tar.gz $(git ls-files)
