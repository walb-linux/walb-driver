# Description

WalB is a block device driver for linux kernel that stores write-ahead logs additionally for efficient backup and replication.

# Directories

* include: header files shared by kernel/userland code.
* module: linux device driver source files.
* doc: documents.
* tool(deprecated): userland tools to control walb devices. use walb-tools repository.
* sim(deprecated): simulators to check algorithm consistency.

# Supported kernel version.

Use an appropriate branch or a tag for your using kernel as follows:

| Branch          | Tag      | Kernel version |
|-----------------|----------|----------------|
| `for-4.8`       | `v1.4.x` | 4.8-4.12       |
| `for-4.3`       | `v1.3.x` | 4.3-4.7        |
| `for-3.14`      | `v1.2.x` | 3.14-4.2       |
| `for-3.10`      | `v1.1.x` | 3.10-3.13      |
| `for-3.2` (EOL) | `v1.0.x` | 3.2-3.8        |

# License

GPL version 2 or 3.

# Copyright

(C) 2010 Cybozu Labs, Inc.

