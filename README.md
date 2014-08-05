# Description

WalB is a block device driver for linux kernel that stores write-ahead logs additionally for efficient backup and replication.

# License

GPL version 2 or 3.

# Directories

* include: header files shared by kernel/userland code.
* module: linux device driver source files.
* tool: userland tools to control walb devices. (deprecated).
* sim: simulators to check algorithm consistency (deprecated).
* doc: documents.

# Supported kernel version.

* This version (1.2.x) supports Linux kernel 3.14 to 3.16.
* For 3.10 to 3.13, use version 1.1.x.
* For 3.2 to 3.8, use version 1.0.x.
