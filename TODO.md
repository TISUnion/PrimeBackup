# TODO

## Features

- [ ] List and removed backup with invalid / corrupted files / blobs
- [ ] Import good backups to fix corrupted blobs
- [x] Show diff between 2 backups
- [ ] Record and show restore logs

CDC chunking

- [x] FastCDC 2020 lib
- [x] Backup create
- [x] Backup export
- [x] DB migration
- [x] Blob size display texts updates
- [x] DB Validation tasks
- [x] DB Prune tasks
- [ ] Performance optimization
- [ ] Fuse support
- [ ] TODO cleanup
- [x] Compress migration
- [ ] Database inspection (blob, chunk, chunk group)
- [ ] Document

## QOL

- [x] More restrictive `!!pb confirm` and `!!pb abort`
- [ ] Prune DB backups

## Documents

- [ ] Command usages
- [ ] Implementation detail references

## Extensibility

- [ ] Provides HTTP API on tcp or unix socket
- [x] CLI tools supports create FUSE file system with [python-fuse](https://github.com/libfuse/python-fuse)
