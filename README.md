# Description

WalB is a block device driver for linux kernel that stores write-ahead logs additionally for efficient backup and replication.

# License

GPL version 2 or 3.

# Directories

* include: header files shared by kernel/userland code.
* module: linux device driver source files.
* doc: documents.
* tool(deprecated): userland tools to control walb devices.
* sim(deprecated): simulators to check algorithm consistency.

# Supported kernel version.

Use an appropriate branch or a tag for your using kernel as follows:

| Branch     | Tag      | Kernel version |
|------------|----------|----------------|
| `master`   | `v1.2.x` | 3.14-          |
| `for-3.13` | `v1.1.x` | 3.10-3.13      |
| `for-3.2`  | `v1.0.x` | 3.2-3.8        |

